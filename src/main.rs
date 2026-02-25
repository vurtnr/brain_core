use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use calamine::{open_workbook, DataType, Reader, Xlsx};
use futures::future::join_all;
use image::{imageops::FilterType, DynamicImage, GenericImage};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::env;
use std::fs;
use std::io::Cursor;
use std::time::Duration;

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
    #[serde(rename = "isAd")]
    is_ad: bool,
    #[serde(rename = "cosScore")]
    cos_score: f64,
}

#[derive(Debug, Deserialize)]
struct VlmResponse {
    #[serde(default)]
    reasoning: String, // 👈 新增：用来接收大模型的分析过程
    match_ids: Vec<usize>,
}

// ==========================================
// 2. 图像处理引擎 (九宫格魔法)
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

    // 👇 修改点：生成纯白背景的画布，而不是透明/黑色
    let mut canvas_img = image::RgbaImage::from_pixel(canvas_size, canvas_size, image::Rgba([255, 255, 255, 255]));
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
    let base64_str = BASE64_STANDARD.encode(buffer.into_inner());

    Some(format!("data:image/jpeg;base64,{}", base64_str))
}

// ==========================================
// 3. 阿里云千问视觉模型 (Qwen-VL) 交互
// ==========================================
// 👇 修改点：函数签名增加 valid_count 参数，告诉大模型到底有几个商品
async fn verify_with_qwen_vl(
    client: &Client,
    ozon_image_base64: &str,
    grid_base64: &str,
    valid_count: usize,
) -> Vec<usize> {
    let api_key = env::var("DASHSCOPE_API_KEY").expect("❌ 找不到 DASHSCOPE_API_KEY 环境变量！");
    let api_url = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions";
    let model_name = "qwen3-vl-plus";

    // 👇 升级 1：角色设定更加严厉，要求先分析后结论
    let system_prompt = "你是一个极其严谨、拥有10年经验的跨国采购专家。任务是进行『SKU级别的同款商品鉴定』。请严格只输出 JSON 格式。";

    // 👇 升级 2：强制大模型使用“思维链 (Chain of Thought)”进行逐一比对
    let user_prompt = format!("图 A 是目标商品原图。图 B 是由候选商品拼成的九宫格（按从左到右、从上到下，编号 1 到 9）。
🚨 特别注意：图 B 中只有前 {} 个格子有商品，其余为空白！

鉴定规则：
1. 忽略背景不同、文字语言差异、牛皮癣水印。
2. 极其严格地核对【物理模具、角色形态（如动物种类）、核心结构、武器】。不同动物或不同形态绝对不是同款！

输出要求：
请务必先给出你的详细对比分析，然后再给出结论。严格按照以下 JSON 格式输出：
{{
  \"reasoning\": \"图1与图A的主体皆为XXX，武器一致；图2主体为犀牛，与图A的蝎子不符，排除...\",
  \"match_ids\": [1]
}}
", valid_count);

    // 👇 升级 3：注入 temperature: 0.01 (几乎为0)，彻底消除大模型的随机幻觉
    let payload = json!({
        "model": model_name,
        "temperature": 0.01,
        "response_format": { "type": "json_object" },
        "messages": [
            { "role": "system", "content": system_prompt },
            { "role": "user", "content": [
                { "type": "text", "text": user_prompt },
                { "type": "text", "text": "【图 A: 目标原图】" },
                { "type": "image_url", "image_url": { "url": ozon_image_base64 } },
                { "type": "text", "text": "【图 B: 候选九宫格】" },
                { "type": "image_url", "image_url": { "url": grid_base64 } }
            ]}
        ]
    });

    let res = client.post(api_url).header("Authorization", format!("Bearer {}", api_key)).json(&payload).send().await;

    match res {
        Ok(response) => {
            if let Ok(body) = response.json::<serde_json::Value>().await {
                if let Some(content) = body["choices"][0]["message"]["content"].as_str() {
                    // 解析 JSON 提取 match_ids 和推理过程
                    if let Ok(vlm_res) = serde_json::from_str::<VlmResponse>(content) {
                        println!("==================================================");
                        println!("💡 【大模型深度思考过程】:\n{}", vlm_res.reasoning); // 👈 打印大模型的逻辑
                        println!("==================================================");
                        return vlm_res.match_ids;
                    } else {
                        eprintln!("⚠️ 模型返回了非预期 JSON: {}", content);
                    }
                }
            }
        },
        Err(e) => eprintln!("❌ 网络请求失败: {}", e),
    }
    vec![]
}

// ==========================================
// 4. 商业决断核心逻辑
// ==========================================
fn parse_price(price_str: &str) -> f64 {
    let cleaned = price_str.replace("¥", "").replace(",", "").trim().to_string();
    cleaned.parse::<f64>().unwrap_or(f64::MAX)
}

fn find_cheapest(candidates: Vec<Candidate>) -> Option<Candidate> {
    let mut valid_items = candidates;
    if valid_items.is_empty() { return None; }
    valid_items.sort_by(|a, b| parse_price(&a.price).partial_cmp(&parse_price(&b.price)).unwrap_or(std::cmp::Ordering::Equal));
    Some(valid_items[0].clone())
}

// ==========================================
// 5. 调度总枢纽
// ==========================================
#[tokio::main]
async fn main() {
    let client = Client::new();
    println!("🚀 [Rust Brain] 启动跨国搜图比价系统...");

    // 1. 初始化 Excel 读取器
    let mut excel: Xlsx<_> = open_workbook("1.xlsx")
        .expect("❌ 无法读取 Excel 文件，请确保 1.xlsx 在项目根目录下！");

    if let Some(Ok(range)) = excel.worksheet_range_at(0) {
        let total_rows = range.rows().count();
        println!("📊 成功读取 Excel，共发现 {} 行数据 (包含空行)", total_rows);

        // 2. 遍历 Excel 里的每一行
        for (row_index, row) in range.rows().enumerate() {
            if row_index == 0 { continue; } // 跳过表头

            // 提取第 2 列的 SKU (索引为 1)
            let ozon_sku = match row.get(1) {
                Some(DataType::String(s)) => s.trim().to_string(),
                Some(DataType::Float(f)) => f.to_string(),
                Some(DataType::Int(i)) => i.to_string(),
                _ => String::new(),
            };

            // 🛡️ 坚决过滤空行或幽灵数据
            if ozon_sku.is_empty() || ozon_sku == "UNKNOWN_SKU" || ozon_sku.len() < 3 {
                continue;
            }

            println!("\n==================================================");
            println!("🎯 正在处理 Excel 第 {} 行数据，Ozon SKU: {}", row_index + 1, ozon_sku);

            // 3. 智能读取本地 Ozon 目标图片 (支持 jpg 和 png)
            let mut ozon_img_bytes = Vec::new();
            let mut format_ext = "jpeg";

            let path_jpg = format!("./images/SKU_{}.jpg", ozon_sku);
            let path_png = format!("./images/SKU_{}.png", ozon_sku);

            if let Ok(bytes) = fs::read(&path_jpg) {
                ozon_img_bytes = bytes;
            } else if let Ok(bytes) = fs::read(&path_png) {
                ozon_img_bytes = bytes;
                format_ext = "png";
            } else {
                eprintln!("⚠️ 找不到本地图片，已尝试路径: {} 和 {}，跳过该 SKU！", path_jpg, path_png);
                continue;
            }

            let ozon_base64 = format!("data:image/{};base64,{}", format_ext, BASE64_STANDARD.encode(ozon_img_bytes));
            println!("✅ 成功读取本地原图并完成 Base64 转码");

            // 4. [此处为联动 Node.js 的占位符，当前使用 Mock 数据测试]
            let mock_json = r#"[
                {
                    "title": "正品盟卡车神之魔幻元珠3...",
                    "price": "¥43",
                    "itemUrl": "https://detail.1688.com/offer/857252926357.html",
                    "imageUrl": "https://cbu01.alicdn.com/img/ibank/O1CN01AIcNGQ1kn1HM1hG7m_!!2218627154727-0-cib.jpg_460x460q100.jpg_.webp",
                    "isAd": false,
                    "cosScore": 0.862
                },
                {
                    "title": "神战角犀犀牛空巨神对决玩具车...",
                    "price": "¥80",
                    "itemUrl": "https://detail.1688.com/offer/904800800155.html",
                    "imageUrl": "https://cbu01.alicdn.com/img/ibank/O1CN01fYGDeV23mzvXcjKMY_!!2219457737299-0-cib.jpg_460x460q100.jpg_.webp",
                    "isAd": false,
                    "cosScore": 0.934
                }
            ]"#;

            let candidates: Vec<Candidate> = serde_json::from_str(mock_json).unwrap_or_default();

            // 5. 生成 1688 候选商品九宫格
            println!("🎨 正在并发下载 1688 图片并生成九宫格矩阵...");
            let grid_base64 = match create_grid_base64(&client, &candidates).await {
                Some(b64) => b64,
                None => {
                    eprintln!("❌ 拼图失败，网络下载异常！");
                    continue;
                }
            };

            // 6. 呼叫大模型决断
            println!("🧠 呼叫 Qwen3-VL 进行同款鉴定...");
            // 👇 修改点：计算实际传入的商品数量（最多9个）
            let valid_count = std::cmp::min(candidates.len(), 9);
            let match_ids = verify_with_qwen_vl(&client, &ozon_base64, &grid_base64, valid_count).await;
            println!("🎯 千问选出的真实同款网格编号: {:?}", match_ids);

            // 7. 价格击穿
            let mut verified_candidates = Vec::new();
            for &id in &match_ids {
                if id >= 1 && id <= candidates.len() {
                    verified_candidates.push(candidates[id - 1].clone());
                }
            }

            if let Some(cheapest) = find_cheapest(verified_candidates) {
                println!("🏆 找到全网最低价同款！准备回填 Excel：成本 [{}], 链接 [{}]", cheapest.price, cheapest.item_url);
            } else {
                println!("⚠️ 经大模型严审：当前批次无一真实同款，全部过滤。");
            }
        }
    } else {
        eprintln!("❌ 无法解析 Excel 的工作表");
    }

    println!("\n✅ 所有 Excel 任务处理完毕！");
}
