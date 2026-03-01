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
use std::time::{Duration, Instant};
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
        if let (Some(cap_name), Some(cap_embed)) =
            (re_name.captures(block), re_embed.captures(block))
        {
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
    if let Ok(resp) = client
        .get(url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
    {
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
    let canvas_img =
        image::RgbaImage::from_pixel(canvas_size, canvas_size, image::Rgba([255, 255, 255, 255]));
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

    if !has_image {
        return None;
    }
    let mut buffer = Cursor::new(Vec::new());
    canvas
        .write_to(&mut buffer, image::ImageFormat::Jpeg)
        .ok()?;
    Some(format!(
        "data:image/jpeg;base64,{}",
        BASE64_STANDARD.encode(buffer.into_inner())
    ))
}

async fn fetch_1688_candidates(
    client: &Client,
    image_path: &str,
    force_full_crop: bool,
) -> Option<Vec<Candidate>> {
    let node_api = "http://127.0.0.1:8266/search";
    let payload = json!({ "imagePath": image_path, "forceFullCrop": force_full_crop });
    const NODE_TIMEOUT_SECS: u64 = 90;
    const MAX_NODE_ATTEMPTS: usize = 3;

    for attempt in 1..=MAX_NODE_ATTEMPTS {
        match client
            .post(node_api)
            .json(&payload)
            .timeout(Duration::from_secs(NODE_TIMEOUT_SECS))
            .send()
            .await
        {
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
                        }
                        Err(e) => {
                            println!(
                                "🛑 [防劫持警报] JSON 解析失败(第 {}/{} 次): {}",
                                attempt, MAX_NODE_ATTEMPTS, e
                            );
                        }
                    }
                } else {
                    let err_text = res.text().await.unwrap_or_default();
                    println!(
                        "🛑 遭遇非 200 状态码(第 {}/{} 次): {}",
                        attempt, MAX_NODE_ATTEMPTS, status
                    );
                    println!(
                        "🚨 错误详情: {}",
                        err_text.chars().take(200).collect::<String>()
                    );
                }
            }
            Err(e) => {
                println!(
                    "🚨 严重网络故障：无法连接 Node.js (第 {}/{} 次)！",
                    attempt, MAX_NODE_ATTEMPTS
                );
                println!("错误详情: {}", e);
            }
        }

        if attempt >= MAX_NODE_ATTEMPTS {
            return None;
        }
        println!("⚠️ 等待 3 秒后重试...");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    None
}

async fn verify_with_qwen_vl(
    client: &Client,
    ozon_image_base64: &str,
    grid_base64: &str,
    valid_count: usize,
    ozon_name_opt: Option<&str>,
) -> Option<Vec<usize>> {
    let api_key = env::var("DASHSCOPE_API_KEY").expect("❌ 找不到 DASHSCOPE_API_KEY");
    let api_url = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions";

    let system_prompt;
    let user_prompt;

    if let Some(name) = ozon_name_opt {
        system_prompt = "你是SKU同款鉴定器。只返回JSON，不要输出额外说明。";
        user_prompt = format!(
            "图 A 是目标商品原图。图 B 是候选商品九宫格（编号 1 到 9）。\n\
            🚨 图 B 中只有前 {} 个格子有商品！\n\
            🚨 商品名称参考：【{}】。\n\
            规则：\n\
            1. 只比较商品主体的物理结构/模具，忽略背景、文字、水印、角度。\n\
            2. 若是同款就返回编号；无同款返回空数组。\n\
            严格输出 JSON：\n\
            {{\n  \"reasoning\": \"简短结论\",\n  \"match_ids\": [1]\n}}",
            valid_count, name
        );
    } else {
        system_prompt = "你是SKU同款鉴定器。只返回JSON，不要输出额外说明。";
        user_prompt = format!(
            "图 A 是目标商品原图。图 B 是候选商品九宫格（编号 1 到 9）。\n\
            🚨 图 B 中只有前 {} 个格子有商品！\n\
            规则：\n\
            1. 只比较商品主体的物理结构/模具，忽略背景、文字、水印、角度。\n\
            2. 若是同款就返回编号；无同款返回空数组。\n\
            严格输出 JSON：\n\
            {{\n  \"reasoning\": \"简短结论\",\n  \"match_ids\": [1]\n}}",
            valid_count
        );
    }

    let payload = json!({
        "model": "qwen3-vl-plus",
        "temperature": 0.01,
        "max_tokens": 220,
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

    match client
        .post(api_url)
        .header("Authorization", format!("Bearer {}", api_key))
        .json(&payload)
        .timeout(Duration::from_secs(30))
        .send()
        .await
    {
        Ok(response) => {
            let status = response.status();
            if !status.is_success() {
                let err_text = response.text().await.unwrap_or_default();
                println!(
                    "💥 大模型 API 崩溃或被限流！状态码: {} \n详情: {}",
                    status, err_text
                );
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
        }
        Err(e) => {
            println!("💥 大模型网络请求失败: {}", e);
            None
        }
    }
}

fn parse_price_value(price: &str) -> Option<f64> {
    let cleaned = price.replace(['¥', ','], "");
    let mut numbers = Vec::new();
    let mut current = String::new();

    for ch in cleaned.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            current.push(ch);
        } else if !current.is_empty() {
            if let Ok(value) = current.parse::<f64>() {
                if value.is_finite() {
                    numbers.push(value);
                }
            }
            current.clear();
        }
    }

    if !current.is_empty() {
        if let Ok(value) = current.parse::<f64>() {
            if value.is_finite() {
                numbers.push(value);
            }
        }
    }

    numbers.into_iter().reduce(f64::min)
}

fn parse_positive_price_value(price: &str) -> Option<f64> {
    parse_price_value(price).filter(|value| *value > 0.0)
}

fn price_sort_key(price: &str) -> f64 {
    parse_positive_price_value(price).unwrap_or(f64::MAX)
}

fn sort_candidates_by_price(candidates: &mut [Candidate]) {
    candidates.sort_by(|a, b| {
        price_sort_key(&a.price)
            .partial_cmp(&price_sort_key(&b.price))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
}

fn build_verification_chunks(mut candidates: Vec<Candidate>) -> Vec<Vec<Candidate>> {
    if candidates.is_empty() {
        return Vec::new();
    }

    sort_candidates_by_price(&mut candidates);
    candidates
        .into_iter()
        .take(27)
        .collect::<Vec<_>>()
        .chunks(9)
        .map(|chunk| chunk.to_vec())
        .collect()
}

#[derive(Debug)]
struct StabilityMetrics {
    jaccard: f64,
    cheapest_id_stable: bool,
}

fn normalize_match_ids(match_ids: &[usize], valid_count: usize) -> Vec<usize> {
    let mut ids = match_ids
        .iter()
        .copied()
        .filter(|id| *id >= 1 && *id <= valid_count)
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn collect_matched_candidates(chunk: &[Candidate], match_ids: &[usize]) -> Vec<Candidate> {
    normalize_match_ids(match_ids, chunk.len())
        .into_iter()
        .map(|id| chunk[id - 1].clone())
        .collect()
}

fn min_price_in_candidates(candidates: &[Candidate]) -> Option<f64> {
    candidates
        .iter()
        .filter_map(|c| parse_positive_price_value(&c.price))
        .reduce(f64::min)
}

fn min_price_in_chunk(chunk: &[Candidate]) -> Option<f64> {
    chunk
        .iter()
        .filter_map(|c| parse_positive_price_value(&c.price))
        .reduce(f64::min)
}

fn compute_gap_ratio(current_best_price: f64, next_chunk_min_price: Option<f64>) -> Option<f64> {
    if current_best_price <= 0.0 {
        return None;
    }
    next_chunk_min_price.map(|next_price| (next_price - current_best_price) / current_best_price)
}

fn should_probe_next_chunk(
    hit_count: usize,
    stability: Option<&StabilityMetrics>,
    gap_ratio: Option<f64>,
    has_next_chunk: bool,
) -> bool {
    const SINGLE_HIT_GAP_THRESHOLD: f64 = 0.12;
    const TWO_HIT_STABILITY_THRESHOLD: f64 = 0.5;
    const SINGLE_HIT_GAP_THRESHOLD_NO_STABILITY: f64 = 0.08;
    const TWO_HIT_GAP_THRESHOLD_NO_STABILITY: f64 = 0.03;

    if !has_next_chunk {
        return false;
    }
    if hit_count == 0 || hit_count >= 3 {
        return false;
    }

    let Some(stability) = stability else {
        return match hit_count {
            1 => gap_ratio
                .map(|gap| gap < SINGLE_HIT_GAP_THRESHOLD_NO_STABILITY)
                .unwrap_or(false),
            2 => gap_ratio
                .map(|gap| gap < TWO_HIT_GAP_THRESHOLD_NO_STABILITY)
                .unwrap_or(false),
            _ => false,
        };
    };

    if hit_count == 2 {
        return !(stability.jaccard >= TWO_HIT_STABILITY_THRESHOLD && stability.cheapest_id_stable);
    }

    if stability.jaccard >= 1.0 && stability.cheapest_id_stable {
        return gap_ratio
            .map(|gap| gap < SINGLE_HIT_GAP_THRESHOLD)
            .unwrap_or(false);
    }

    true
}

fn find_cheapest(candidates: Vec<Candidate>) -> Option<Candidate> {
    let mut valid_items = candidates;
    if valid_items.is_empty() {
        return None;
    }
    sort_candidates_by_price(&mut valid_items);
    valid_items
        .into_iter()
        .find(|item| parse_positive_price_value(&item.price).is_some())
}

async fn process_candidates(
    client: &Client,
    ozon_base64: &str,
    candidates: Vec<Candidate>,
    ozon_name_opt: Option<&str>,
) -> Result<Option<Candidate>, &'static str> {
    let chunks = build_verification_chunks(candidates);
    if chunks.is_empty() {
        return Ok(None);
    }

    let mut pending_low_confidence_matches: Option<Vec<Candidate>> = None;

    for (chunk_index, chunk) in chunks.iter().enumerate() {
        if let Some(grid_base64) = create_grid_base64(client, chunk).await {
            let verify_result = verify_with_qwen_vl(
                client,
                ozon_base64,
                &grid_base64,
                chunk.len(),
                ozon_name_opt,
            )
            .await;

            let match_ids = match verify_result {
                Some(ids) => ids,
                None => {
                    if let Some(pending) = pending_low_confidence_matches.take() {
                        println!("⚠️ 复查组模型请求失败，采用上一组低价命中结果。");
                        return Ok(find_cheapest(pending));
                    }
                    return Err("大模型API调用异常/超时");
                }
            };

            let matched_candidates = collect_matched_candidates(chunk, &match_ids);

            if let Some(mut pending) = pending_low_confidence_matches.take() {
                if !matched_candidates.is_empty() {
                    pending.extend(matched_candidates);
                } else {
                    println!("✅ 复查下一组未新增命中，采用上一组低价命中结果。");
                }
                println!("✅ 已完成低置信兜底复查，停止后续组比对。");
                return Ok(find_cheapest(pending));
            }

            if matched_candidates.is_empty() {
                continue;
            }

            let has_next_chunk = chunk_index + 1 < chunks.len();

            let gap_ratio = min_price_in_candidates(&matched_candidates).and_then(|best_price| {
                compute_gap_ratio(
                    best_price,
                    chunks
                        .get(chunk_index + 1)
                        .and_then(|c| min_price_in_chunk(c)),
                )
            });

            if should_probe_next_chunk(
                matched_candidates.len(),
                None,
                gap_ratio,
                has_next_chunk,
            ) {
                println!(
                    "⚠️ 第 {} 组触发价格邻近复查(hit_count={}，gap_ratio={:?})。",
                    chunk_index + 1,
                    matched_candidates.len(),
                    gap_ratio
                );
                pending_low_confidence_matches = Some(matched_candidates);
                continue;
            }

            println!(
                "✅ 在第 {} 组(按价格升序)命中同款，停止后续组比对。",
                chunk_index + 1
            );
            return Ok(find_cheapest(matched_candidates));
        } else if let Some(pending) = pending_low_confidence_matches.take() {
            println!("⚠️ 复查组图片下载失败，采用上一组低价命中结果。");
            return Ok(find_cheapest(pending));
        }
    }

    if let Some(pending) = pending_low_confidence_matches.take() {
        return Ok(find_cheapest(pending));
    }

    Ok(None)
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
    let formula_range = excel
        .worksheet_formula(&sheet_name)
        .and_then(|res| res.ok());

    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    let mut current_write_row = 0;
    let header_format = Format::new().set_bold().set_background_color(Color::Silver);

    let re_id = Regex::new(r#"ID_[A-Za-z0-9]{32}"#).unwrap();

    if let Some(Ok(range)) = excel.worksheet_range_at(0) {
        let col_len = range.rows().next().unwrap().len() as u16;
        for (col_idx, header) in range.rows().next().unwrap().iter().enumerate() {
            let _ = worksheet.write_string_with_format(
                0,
                col_idx as u16,
                &header.to_string(),
                &header_format,
            );
        }
        let _ = worksheet.write_string_with_format(0, col_len, "1688成本价", &header_format);
        let _ = worksheet.write_string_with_format(0, col_len + 1, "1688链接", &header_format);
        let _ = worksheet.write_string_with_format(0, col_len + 2, "AI分析结论", &header_format);
        let _ = worksheet.write_string_with_format(0, col_len + 3, "图像比对耗时", &header_format);
        current_write_row += 1;

        for (row_index, row) in range.rows().enumerate().skip(1) {
            for (col_idx, cell) in row.iter().enumerate() {
                let _ =
                    worksheet.write_string(current_write_row, col_idx as u16, &cell.to_string());
            }

            let ozon_name = row
                .get(0)
                .map(|c| c.to_string().trim().to_string())
                .unwrap_or_default();
            let ozon_sku = row
                .get(1)
                .map(|c| c.to_string().trim().to_string())
                .unwrap_or_default();

            if ozon_sku.is_empty() || ozon_sku == "UNKNOWN_SKU" || ozon_sku.len() < 3 {
                current_write_row += 1;
                continue;
            }

            println!("\n==================================================");
            println!(
                "🎯 处理 Excel 第 {} 行 | Ozon SKU: {} | 商品: {}",
                row_index + 1,
                ozon_sku,
                ozon_name
            );

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
                    println!(
                        "⚠️ 第 {} 行未能从 Excel 提取到嵌入图片，跳过",
                        row_index + 1
                    );
                    let _ = worksheet.write_string(current_write_row, col_len + 2, "Excel中无图");
                    current_write_row += 1;
                    continue;
                }
            };

            let abs_img_path = temp_dir.join(format!("SKU_{}.jpg", ozon_sku));
            fs::write(&abs_img_path, &img_bytes).unwrap();
            let target_image_path = abs_img_path.to_string_lossy().to_string();
            let ozon_base64 = format!(
                "data:image/jpeg;base64,{}",
                BASE64_STANDARD.encode(&img_bytes)
            );

            let mut final_cheapest = None;
            let final_status_msg;
            let compare_started_at = Instant::now();

            println!("🌐 [第一重召回] 呼叫 Bun 获取 1688 默认框选数据...");

            if let Some(candidates_pass1) =
                fetch_1688_candidates(&client, &target_image_path, false).await
            {
                if !candidates_pass1.is_empty() {
                    match process_candidates(&client, &ozon_base64, candidates_pass1, None).await {
                        Ok(Some(cheapest)) => {
                            println!("✅ 第一重召回成功锁定最低价！");
                            final_cheapest = Some(cheapest);
                            final_status_msg = "AI比对成功(一次召回)".to_string();
                        }
                        Ok(None) => {
                            println!("⚠️ 警告：第一重视觉召回(3轮)确认无同款，触发二次重绘！");

                            if let Some(candidates_pass2) =
                                fetch_1688_candidates(&client, &target_image_path, true).await
                            {
                                match process_candidates(
                                    &client,
                                    &ozon_base64,
                                    candidates_pass2,
                                    Some(&ozon_name),
                                )
                                .await
                                {
                                    Ok(Some(cheapest)) => {
                                        println!("🏆 绝杀！大模型利用『语义排错』，在二次全图中成功揪出真同款！");
                                        final_cheapest = Some(cheapest);
                                        final_status_msg = "AI比对成功(二次全图召回)".to_string();
                                    }
                                    Ok(None) => {
                                        println!("❌ 两次召回均无果，确认为无同款。");
                                        final_status_msg = "无真实同款(两轮兜底)".to_string();
                                    }
                                    Err(e) => {
                                        println!("⚠️ 第二重召回时大模型崩溃中断: {}", e);
                                        final_status_msg = format!("大模型API异常: {}", e);
                                    }
                                }
                            } else {
                                println!("⚠️ Node.js 第二次重绘故障。");
                                final_status_msg = "Node爬虫二次获取失败".to_string();
                            }
                        }
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

            let compare_elapsed_secs = compare_started_at.elapsed().as_secs_f64();
            let compare_elapsed_text = format!("{:.2}s", compare_elapsed_secs);
            println!("⏱️ 本条图像比对总耗时: {}", compare_elapsed_text);

            if let Some(cheapest) = final_cheapest {
                println!(
                    "💰 最终底价: {}, 链接: {}",
                    cheapest.price, cheapest.item_url
                );
                let _ = worksheet.write_string(current_write_row, col_len, &cheapest.price);
                let _ = worksheet.write_string(current_write_row, col_len + 1, &cheapest.item_url);
                let _ = worksheet.write_string(current_write_row, col_len + 2, &final_status_msg);
                let _ =
                    worksheet.write_string(current_write_row, col_len + 3, &compare_elapsed_text);
            } else {
                let _ = worksheet.write_string(current_write_row, col_len + 2, &final_status_msg);
                let _ =
                    worksheet.write_string(current_write_row, col_len + 3, &compare_elapsed_text);
            }

            current_write_row += 1;
        }
    }

    workbook.save("result.xlsx").expect("❌ 写入结果失败");
    println!(
        "\n🎉 自动化寻源任务结束！结果已保存至 result.xlsx，临时图片存在 ./temp_images 目录。"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate_with_price(price: &str) -> Candidate {
        Candidate {
            title: "t".to_string(),
            price: price.to_string(),
            item_url: "u".to_string(),
            image_url: "i".to_string(),
        }
    }

    #[test]
    fn parse_price_value_extracts_min_from_range() {
        let price = parse_price_value("¥12.5-18.0");
        assert_eq!(price, Some(12.5));
    }

    #[test]
    fn parse_price_value_returns_none_for_non_numeric() {
        let price = parse_price_value("面议");
        assert!(price.is_none());
    }

    #[test]
    fn parse_positive_price_value_rejects_zero() {
        assert_eq!(parse_positive_price_value("¥0"), None);
        assert_eq!(parse_positive_price_value("¥0.00"), None);
        assert_eq!(parse_positive_price_value("¥0.01"), Some(0.01));
    }

    #[test]
    fn sort_candidates_by_price_keeps_lowest_first_and_non_numeric_last() {
        let mut candidates = vec![
            candidate_with_price("面议"),
            candidate_with_price("¥19.8"),
            candidate_with_price("¥12.0-13.0"),
            candidate_with_price("¥15"),
        ];

        sort_candidates_by_price(&mut candidates);

        assert_eq!(candidates[0].price, "¥12.0-13.0");
        assert_eq!(candidates[1].price, "¥15");
        assert_eq!(candidates[2].price, "¥19.8");
        assert_eq!(candidates[3].price, "面议");
    }

    #[test]
    fn build_verification_chunks_orders_by_price_and_caps_to_27() {
        let mut candidates = Vec::new();
        for i in (1..=30).rev() {
            candidates.push(candidate_with_price(&format!("¥{}", i)));
        }

        let chunks = build_verification_chunks(candidates);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 9);
        assert_eq!(chunks[2].len(), 9);
        assert_eq!(chunks[0][0].price, "¥1");
        assert_eq!(chunks[2][8].price, "¥27");
    }

    #[test]
    fn should_not_probe_when_hit_count_is_three() {
        assert!(!should_probe_next_chunk(3, None, Some(0.2), true));
    }

    #[test]
    fn should_not_probe_when_two_hits_and_stability_good() {
        let stability = StabilityMetrics {
            jaccard: 0.6,
            cheapest_id_stable: true,
        };
        assert!(!should_probe_next_chunk(
            2,
            Some(&stability),
            Some(0.1),
            true
        ));
    }

    #[test]
    fn should_probe_when_two_hits_and_stability_weak() {
        let stability = StabilityMetrics {
            jaccard: 0.4,
            cheapest_id_stable: true,
        };
        assert!(should_probe_next_chunk(
            2,
            Some(&stability),
            Some(0.2),
            true
        ));
    }

    #[test]
    fn should_not_probe_single_hit_without_stability_when_gap_large() {
        assert!(!should_probe_next_chunk(1, None, Some(0.2), true));
    }

    #[test]
    fn should_probe_single_hit_without_stability_when_gap_small() {
        assert!(should_probe_next_chunk(1, None, Some(0.05), true));
    }

    #[test]
    fn should_not_probe_when_single_hit_stable_and_gap_large() {
        let stability = StabilityMetrics {
            jaccard: 1.0,
            cheapest_id_stable: true,
        };
        assert!(!should_probe_next_chunk(
            1,
            Some(&stability),
            Some(0.15),
            true
        ));
    }

    #[test]
    fn should_probe_when_single_hit_stable_but_gap_small() {
        let stability = StabilityMetrics {
            jaccard: 1.0,
            cheapest_id_stable: true,
        };
        assert!(should_probe_next_chunk(
            1,
            Some(&stability),
            Some(0.05),
            true
        ));
    }

    #[test]
    fn should_probe_when_single_hit_unstable() {
        let stability = StabilityMetrics {
            jaccard: 0.0,
            cheapest_id_stable: false,
        };
        assert!(should_probe_next_chunk(
            1,
            Some(&stability),
            Some(0.2),
            true
        ));
    }

    #[test]
    fn should_not_probe_without_next_chunk() {
        let stability = StabilityMetrics {
            jaccard: 0.0,
            cheapest_id_stable: false,
        };
        assert!(!should_probe_next_chunk(
            1,
            Some(&stability),
            Some(0.01),
            false
        ));
    }

    #[test]
    fn compute_gap_ratio_returns_relative_difference() {
        let gap = compute_gap_ratio(100.0, Some(120.0));
        assert_eq!(gap, Some(0.2));
    }

    #[test]
    fn find_cheapest_skips_zero_price_items() {
        let candidates = vec![
            candidate_with_price("¥0"),
            candidate_with_price("¥3.2"),
            candidate_with_price("¥2.5"),
        ];
        let cheapest = find_cheapest(candidates).expect("should find positive price");
        assert_eq!(cheapest.price, "¥2.5");
    }
}
