use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use calamine::{open_workbook, Reader, Xlsx}; 
use futures::future::join_all;
use image::{imageops::FilterType, DynamicImage, GenericImage};
use regex::Regex;
use reqwest::Client;
use rust_xlsxwriter::{Color, Format, Workbook};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::{Cursor, Read};
use std::time::Duration;
use zip::ZipArchive;

// ==========================================
// 1. 数据模型定义
// ==========================================
#[derive(Debug, Deserialize, Serialize, Clone)]
struct Candidate {
    title: String,
    price: String,
    #[serde(rename = "itemUrl")]
    item_url: String,
    #[serde(rename = "imageUrl")]
    image_url: String,
}

#[derive(Debug, Deserialize)]
struct NodeResponse {
    success: bool,
    data: Option<Vec<Candidate>>,
    #[allow(dead_code)] 
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct VlmResponse {
    #[serde(default)]
    reasoning: String,
    match_ids: Vec<usize>,
}

// ==========================================
// 2. Excel 底层图片提取
// ==========================================
fn extract_wps_images(excel_path: &str) -> HashMap<String, Vec<u8>> {
    let file = File::open(excel_path).expect("无法打开 Excel 文件以提取底层图片");
    let mut archive = ZipArchive::new(file).expect("无法解析 Excel ZIP 结构");

    let mut rels_content = String::new();
    if let Ok(mut f) = archive.by_name("xl/_rels/cellimages.xml.rels") {
        let _ = f.read_to_string(&mut rels_content);
    }
    let mut rid_to_target = HashMap::new();
    let re_rel = Regex::new(r#"Id="([^"]+)"[^>]*Target="([^"]+)""#).unwrap();
    for cap in re_rel.captures_iter(&rels_content) {
        rid_to_target.insert(cap[1].to_string(), cap[2].to_string());
    }

    let mut cellimages_content = String::new();
    if let Ok(mut f) = archive.by_name("xl/cellimages.xml") {
        let _ = f.read_to_string(&mut cellimages_content);
    }
    let mut id_to_target = HashMap::new();
    for block in cellimages_content.split("<etc:cellImage>") {
        let re_name = Regex::new(r#"name="([^"]+)""#).unwrap();
        let re_embed = Regex::new(r#"r:embed="([^"]+)""#).unwrap();
        if let (Some(cap_name), Some(cap_embed)) = (re_name.captures(block), re_embed.captures(block)) {
            if let Some(target) = rid_to_target.get(&cap_embed[1]) {
                id_to_target.insert(cap_name[1].to_string(), target.clone());
            }
        }
    }

    let mut image_data = HashMap::new();
    for (id, target) in id_to_target {
        let clean_target = if target.starts_with("../") {
            format!("xl/{}", &target[3..])
        } else {
            format!("xl/{}", target)
        };
        if let Ok(mut f) = archive.by_name(&clean_target) {
            let mut buf = Vec::new();
            if f.read_to_end(&mut buf).is_ok() {
                image_data.insert(id, buf);
            }
        }
    }
    image_data
}

// ==========================================
// 3. 图像处理与 Node/VLM 交互
// ==========================================
async fn fetch_and_resize(client: &Client, url: &str, size: u32) -> Option<DynamicImage> {
    if let Ok(resp) = client.get(url).timeout(Duration::from_secs(10)).send().await {
        if let Ok(bytes) = resp.bytes().await {
            if let Ok(img) = image::load_from_memory(&bytes) {
                return Some(img.resize_exact(size, size, FilterType::Lanczos3));
            }
        }
    }
    None
}

async fn create_grid_base64(client: &Client, candidates: &[Candidate]) -> Option<String> {
    let tile_size = 300;
    let grid_size = 3;
    let canvas_size = tile_size * grid_size;
    let canvas_img = image::RgbaImage::from_pixel(canvas_size, canvas_size, image::Rgba([255, 255, 255, 255]));
    let mut canvas = image::DynamicImage::ImageRgba8(canvas_img);

    let mut tasks = Vec::new();
    for c in candidates.iter().take(9) {
        tasks.push(fetch_and_resize(client, &c.image_url, tile_size));
    }
    
    let downloaded_images = join_all(tasks).await;
    let mut has_image = false;

    for (index, img_opt) in downloaded_images.into_iter().enumerate() {
        if let Some(img) = img_opt {
            has_image = true;
            let x = (index as u32 % grid_size) * tile_size;
            let y = (index as u32 / grid_size) * tile_size;
            let _ = canvas.copy_from(&img, x, y);
        }
    }

    if !has_image { return None; }
    let mut buffer = Cursor::new(Vec::new());
    canvas.write_to(&mut buffer, image::ImageFormat::Jpeg).ok()?;
    Some(format!("data:image/jpeg;base64,{}", BASE64_STANDARD.encode(buffer.into_inner())))
}

async fn fetch_1688_candidates(client: &Client, image_path: &str, force_full_crop: bool) -> Option<Vec<Candidate>> {
    let node_api = "http://127.0.0.1:8266/search";
    let payload = json!({ "imagePath": image_path, "forceFullCrop": force_full_crop });
    
    let mut retry_count = 0;
    loop {
        match client.post(node_api).json(&payload).timeout(Duration::from_secs(180)).send().await {
            Ok(res) => {
                let status = res.status();
                if status.is_success() {
                    let text_body = res.text().await.unwrap_or_default();
                    match serde_json::from_str::<NodeResponse>(&text_body) {
                        Ok(node_res) => {
                            if node_res.success { 
                                return Some(node_res.data.unwrap_or_default()); 
                            } else {
                                println!("🛑 Node.js 返回业务报错: {:?}", node_res.error);
                                return None; 
                            }
                        },
                        Err(e) => {
                            println!("🛑 [防劫持警报] JSON 解析失败: {}", e);
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            continue;
                        }
                    }
                } else {
                    // 🌟 修复关键点：如果遇到 500 代理报错，死磕重试，绝不抛弃数据跳过！
                    let err_text = res.text().await.unwrap_or_default();
                    println!("🛑 遭遇非 200 状态码: {}", status);
                    println!("🚨 错误详情: {}", err_text.chars().take(200).collect::<String>());
                    println!("⚠️ 触发熔断保护，等待 5 秒后重试...");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            }
            Err(e) => {
                retry_count += 1;
                println!("🚨 严重网络故障：无法连接 Node.js (第 {} 次重试)！等待 5 秒...", retry_count);
                println!("错误详情: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue; 
            }
        }
    }
}

async fn verify_with_qwen_vl(client: &Client, ozon_image_base64: &str, grid_base64: &str, valid_count: usize, ozon_name_opt: Option<&str>) -> Option<Vec<usize>> {
    let api_key = env::var("DASHSCOPE_API_KEY").expect("❌ 找不到 DASHSCOPE_API_KEY");
    let api_url = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions";
    
    let system_prompt;
    let user_prompt;

    if let Some(name) = ozon_name_opt {
        system_prompt = "你是一个极其严谨的采购专家。拥有超强的多语言语义理解和图像排查能力。";
        user_prompt = format!(
            "图 A 是目标商品原图。图 B 是候选商品九宫格（编号 1 到 9）。\n\
            🚨 图 B 中只有前 {} 个格子有商品！\n\
            🚨 买家要采购的真实商品名称是：【{}】（请利用你的多语言常识理解该商品的本质特征）。\n\
            规则：\n\
            1. 排除背景里的干扰物（如植物盆栽、桌子、模特等），紧紧围绕上述【商品名称】寻找同款！\n\
            2. 忽略背景不同、文字语言差异、水印。\n\
            3. 极其严格地核对【物理模具、形态、核心结构】。\n\
            请先给出详细对比分析，再给出结论。严格输出 JSON 格式：\n\
            {{\n  \"reasoning\": \"对比过程...\",\n  \"match_ids\": [1]\n}}",
            valid_count, name
        );
    } else {
        system_prompt = "你是一个极其严谨的采购专家。进行SKU级同款视觉鉴定。";
        user_prompt = format!(
            "图 A 是目标商品原图。图 B 是候选商品九宫格（编号 1 到 9）。\n\
            🚨 图 B 中只有前 {} 个格子有商品！\n\
            规则：\n\
            1. 忽略背景不同、文字语言差异、水印。\n\
            2. 极其严格地核对【物理模具、角色形态、核心结构、武器】。\n\
            请先给出详细对比分析，再给出结论。严格输出 JSON 格式：\n\
            {{\n  \"reasoning\": \"对比过程...\",\n  \"match_ids\": [1]\n}}",
            valid_count
        );
    }

    let payload = json!({
        "model": "qwen3-vl-plus",
        "temperature": 0.01,
        "response_format": { "type": "json_object" },
        "messages": [
            { "role": "system", "content": system_prompt },
            { "role": "user", "content": [
                { "type": "text", "text": user_prompt },
                { "type": "image_url", "image_url": { "url": ozon_image_base64 } },
                { "type": "image_url", "image_url": { "url": grid_base64 } }
            ]}
        ]
    });

    match client.post(api_url).header("Authorization", format!("Bearer {}", api_key)).json(&payload).timeout(Duration::from_secs(60)).send().await {
        Ok(response) => {
            let status = response.status();
            if !status.is_success() {
                let err_text = response.text().await.unwrap_or_default();
                println!("💥 大模型 API 崩溃或被限流！状态码: {} \n详情: {}", status, err_text);
                return None; 
            }
            
            if let Ok(body) = response.json::<serde_json::Value>().await {
                if let Some(content) = body["choices"][0]["message"]["content"].as_str() {
                    if let Ok(vlm_res) = serde_json::from_str::<VlmResponse>(content) {
                        println!("💡 深度推理: {}", vlm_res.reasoning.replace('\n', " "));
                        return Some(vlm_res.match_ids);
                    }
                }
            }
            Some(vec![]) 
        },
        Err(e) => {
            println!("💥 大模型网络请求失败: {}", e);
            None 
        }
    }
}

fn find_cheapest(candidates: Vec<Candidate>) -> Option<Candidate> {
    let mut valid_items = candidates;
    if valid_items.is_empty() { return None; }
    valid_items.sort_by(|a, b| {
        let price_a = a.price.replace("¥", "").replace(",", "").trim().parse::<f64>().unwrap_or(f64::MAX);
        let price_b = b.price.replace("¥", "").replace(",", "").trim().parse::<f64>().unwrap_or(f64::MAX);
        price_a.partial_cmp(&price_b).unwrap_or(std::cmp::Ordering::Equal)
    });
    Some(valid_items[0].clone())
}

async fn process_candidates(
    client: &Client, 
    ozon_base64: &str, 
    candidates: Vec<Candidate>,
    ozon_name_opt: Option<&str>
) -> Result<Option<Candidate>, &'static str> {
    if candidates.is_empty() { return Ok(None); }
    
    let mut all_verified_candidates = Vec::new();
    let chunks: Vec<&[Candidate]> = candidates.chunks(9).take(3).collect();
    
    for chunk in chunks {
        if let Some(grid_base64) = create_grid_base64(client, chunk).await {
            match verify_with_qwen_vl(client, ozon_base64, &grid_base64, chunk.len(), ozon_name_opt).await {
                Some(match_ids) => {
                    for &id in &match_ids {
                        if id >= 1 && id <= chunk.len() {
                            all_verified_candidates.push(chunk[id - 1].clone());
                        }
                    }
                },
                None => {
                    return Err("大模型API调用异常/超时"); 
                }
            }
        }
    }
    Ok(find_cheapest(all_verified_candidates))
}

// ==========================================
// 4. 调度总枢纽
// ==========================================
#[tokio::main]
async fn main() {
    // 🌟 核心破局点：使用 .no_proxy() 强制断开与 Clash/VPN 的联系，直达 Node.js！
    let client = reqwest::Client::builder()
        .no_proxy() 
        .build()
        .expect("初始化 HTTP 客户端失败");

    println!("🚀 [Rust Brain] 启动跨国搜图全链路比价系统 (无视代理+熔断重试版)...");

    println!("🔓 正在破解 Excel 底层图片加密库...");
    let wps_images = extract_wps_images("1.xlsx");
    println!("📦 成功从 Excel 中提取到 {} 张隐藏图片！", wps_images.len());

    let current_dir = env::current_dir().unwrap();
    let temp_dir = current_dir.join("temp_images");
    fs::create_dir_all(&temp_dir).unwrap();

    let mut excel: Xlsx<_> = open_workbook("1.xlsx").expect("❌ 无法读取 1.xlsx");
    let sheet_name = excel.sheet_names().get(0).unwrap().clone();
    let formula_range = excel.worksheet_formula(&sheet_name).and_then(|res| res.ok());

    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    let mut current_write_row = 0;
    let header_format = Format::new().set_bold().set_background_color(Color::Silver);

    let re_id = Regex::new(r#"ID_[A-Za-z0-9]{32}"#).unwrap();

    if let Some(Ok(range)) = excel.worksheet_range_at(0) {
        let col_len = range.rows().next().unwrap().len() as u16;
        for (col_idx, header) in range.rows().next().unwrap().iter().enumerate() {
            let _ = worksheet.write_string_with_format(0, col_idx as u16, &header.to_string(), &header_format);
        }
        let _ = worksheet.write_string_with_format(0, col_len, "1688成本价", &header_format);
        let _ = worksheet.write_string_with_format(0, col_len + 1, "1688链接", &header_format);
        let _ = worksheet.write_string_with_format(0, col_len + 2, "AI分析结论", &header_format);
        current_write_row += 1;

        for (row_index, row) in range.rows().enumerate().skip(1) {
            for (col_idx, cell) in row.iter().enumerate() {
                let _ = worksheet.write_string(current_write_row, col_idx as u16, &cell.to_string());
            }

            let ozon_name = row.get(0).map(|c| c.to_string().trim().to_string()).unwrap_or_default();
            let ozon_sku = row.get(1).map(|c| c.to_string().trim().to_string()).unwrap_or_default();
            
            if ozon_sku.is_empty() || ozon_sku == "UNKNOWN_SKU" || ozon_sku.len() < 3 {
                current_write_row += 1;
                continue;
            }

            println!("\n==================================================");
            println!("🎯 处理 Excel 第 {} 行 | Ozon SKU: {} | 商品: {}", row_index + 1, ozon_sku, ozon_name);

            let mut target_img_bytes = None;
            for col_idx in 0..row.len() {
                let cell_str = row[col_idx].to_string();
                if let Some(cap) = re_id.captures(&cell_str) {
                    if let Some(bytes) = wps_images.get(&cap[0]) {
                        target_img_bytes = Some(bytes.clone());
                        break;
                    }
                }
                if let Some(fr) = &formula_range {
                    if let Some(f) = fr.get_value((row_index as u32, col_idx as u32)) {
                        if let Some(cap) = re_id.captures(f) {
                            if let Some(bytes) = wps_images.get(&cap[0]) {
                                target_img_bytes = Some(bytes.clone());
                                break;
                            }
                        }
                    }
                }
            }

            let img_bytes = match target_img_bytes {
                Some(b) => b,
                None => {
                    println!("⚠️ 第 {} 行未能从 Excel 提取到嵌入图片，跳过", row_index + 1);
                    let _ = worksheet.write_string(current_write_row, col_len + 2, "Excel中无图");
                    current_write_row += 1;
                    continue;
                }
            };

            let abs_img_path = temp_dir.join(format!("SKU_{}.jpg", ozon_sku));
            fs::write(&abs_img_path, &img_bytes).unwrap();
            let target_image_path = abs_img_path.to_string_lossy().to_string();
            let ozon_base64 = format!("data:image/jpeg;base64,{}", BASE64_STANDARD.encode(&img_bytes));

            let mut final_cheapest = None;
            let final_status_msg;

            println!("🌐 [第一重召回] 呼叫 Bun 获取 1688 默认框选数据...");
            
            if let Some(candidates_pass1) = fetch_1688_candidates(&client, &target_image_path, false).await {
                if !candidates_pass1.is_empty() {
                    match process_candidates(&client, &ozon_base64, candidates_pass1, None).await {
                        Ok(Some(cheapest)) => {
                            println!("✅ 第一重召回成功锁定最低价！");
                            final_cheapest = Some(cheapest);
                            final_status_msg = "AI比对成功(一次召回)".to_string();
                        },
                        Ok(None) => {
                            println!("⚠️ 警告：第一重视觉召回(3轮)确认无同款，触发二次重绘！");
                            
                            if let Some(candidates_pass2) = fetch_1688_candidates(&client, &target_image_path, true).await {
                                match process_candidates(&client, &ozon_base64, candidates_pass2, Some(&ozon_name)).await {
                                    Ok(Some(cheapest)) => {
                                        println!("🏆 绝杀！大模型利用『语义排错』，在二次全图中成功揪出真同款！");
                                        final_cheapest = Some(cheapest);
                                        final_status_msg = "AI比对成功(二次全图召回)".to_string();
                                    },
                                    Ok(None) => {
                                        println!("❌ 两次召回均无果，确认为无同款。");
                                        final_status_msg = "无真实同款(两轮兜底)".to_string();
                                    },
                                    Err(e) => {
                                        println!("⚠️ 第二重召回时大模型崩溃中断: {}", e);
                                        final_status_msg = format!("大模型API异常: {}", e);
                                    }
                                }
                            } else {
                                println!("⚠️ Node.js 第二次重绘故障。");
                                final_status_msg = "Node爬虫二次获取失败".to_string();
                            }
                        },
                        Err(e) => {
                            println!("⚠️ 第一重召回时大模型崩溃中断: {}", e);
                            final_status_msg = format!("大模型API异常: {}", e);
                        }
                    }
                } else {
                    println!("⚠️ Node.js 未提取到数据 (可能被风控拦截)。");
                    final_status_msg = "爬虫被风控或无数据".to_string();
                }
            } else {
                println!("⚠️ Node.js 爬虫持续断连异常。");
                final_status_msg = "Node爬虫微服务宕机".to_string();
            }

            if let Some(cheapest) = final_cheapest {
                println!("💰 最终底价: {}, 链接: {}", cheapest.price, cheapest.item_url);
                let _ = worksheet.write_string(current_write_row, col_len, &cheapest.price);
                let _ = worksheet.write_string(current_write_row, col_len + 1, &cheapest.item_url);
                let _ = worksheet.write_string(current_write_row, col_len + 2, &final_status_msg);
            } else {
                let _ = worksheet.write_string(current_write_row, col_len + 2, &final_status_msg);
            }

            current_write_row += 1;
        }
    }

    workbook.save("result.xlsx").expect("❌ 写入结果失败");
    println!("\n🎉 自动化寻源任务结束！结果已保存至 result.xlsx，临时图片存在 ./temp_images 目录。");
}