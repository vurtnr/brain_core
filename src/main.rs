use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use calamine::{open_workbook, Reader, Xlsx};
use futures::future::join_all;
use image::{imageops::FilterType, DynamicImage, GenericImage};
use regex::Regex;
use reqwest::Client;
use rust_xlsxwriter::{Color, Format, Workbook};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::io::{Cursor, Read};
use std::sync::OnceLock;
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

const GRID_CANDIDATE_SIZE: usize = 9;
const MAX_VERIFY_GROUPS: usize = 4;
const MAX_VERIFY_CANDIDATES: usize = GRID_CANDIDATE_SIZE * MAX_VERIFY_GROUPS;
const FINAL_REVIEW_CANDIDATE_LIMIT: usize = 8;

enum MatchSummary {
    NoMatch,
    Cheapest(Candidate),
    MatchedButPriceUnavailable { total_matches: usize },
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

fn draw_filled_rect(
    canvas: &mut image::DynamicImage,
    x: u32,
    y: u32,
    width: u32,
    height: u32,
    color: image::Rgba<u8>,
) {
    let x_end = (x + width).min(canvas.width());
    let y_end = (y + height).min(canvas.height());
    for py in y..y_end {
        for px in x..x_end {
            canvas.put_pixel(px, py, color);
        }
    }
}

fn draw_digit(
    canvas: &mut image::DynamicImage,
    x: u32,
    y: u32,
    digit: u32,
    scale: u32,
    color: image::Rgba<u8>,
) {
    const DIGIT_FONT_3X5: [[[u8; 3]; 5]; 10] = [
        [[1, 1, 1], [1, 0, 1], [1, 0, 1], [1, 0, 1], [1, 1, 1]],
        [[0, 1, 0], [1, 1, 0], [0, 1, 0], [0, 1, 0], [1, 1, 1]],
        [[1, 1, 1], [0, 0, 1], [1, 1, 1], [1, 0, 0], [1, 1, 1]],
        [[1, 1, 1], [0, 0, 1], [1, 1, 1], [0, 0, 1], [1, 1, 1]],
        [[1, 0, 1], [1, 0, 1], [1, 1, 1], [0, 0, 1], [0, 0, 1]],
        [[1, 1, 1], [1, 0, 0], [1, 1, 1], [0, 0, 1], [1, 1, 1]],
        [[1, 1, 1], [1, 0, 0], [1, 1, 1], [1, 0, 1], [1, 1, 1]],
        [[1, 1, 1], [0, 0, 1], [0, 0, 1], [0, 0, 1], [0, 0, 1]],
        [[1, 1, 1], [1, 0, 1], [1, 1, 1], [1, 0, 1], [1, 1, 1]],
        [[1, 1, 1], [1, 0, 1], [1, 1, 1], [0, 0, 1], [1, 1, 1]],
    ];

    let Some(pattern) = DIGIT_FONT_3X5.get(digit as usize) else {
        return;
    };

    for (row_idx, row) in pattern.iter().enumerate() {
        for (col_idx, bit) in row.iter().enumerate() {
            if *bit == 1 {
                draw_filled_rect(
                    canvas,
                    x + col_idx as u32 * scale,
                    y + row_idx as u32 * scale,
                    scale,
                    scale,
                    color,
                );
            }
        }
    }
}

fn draw_tile_index_label(
    canvas: &mut image::DynamicImage,
    tile_index: usize,
    tile_size: u32,
    grid_size: u32,
) {
    let x = (tile_index as u32 % grid_size) * tile_size;
    let y = (tile_index as u32 / grid_size) * tile_size;

    draw_filled_rect(
        canvas,
        x + 10,
        y + 10,
        46,
        34,
        image::Rgba([255, 255, 255, 220]),
    );
    draw_digit(
        canvas,
        x + 23,
        y + 16,
        (tile_index + 1) as u32,
        4,
        image::Rgba([0, 0, 0, 255]),
    );
}

async fn create_grid_base64(client: &Client, candidates: &[Candidate]) -> Option<String> {
    let tile_size = 300;
    let grid_size = (GRID_CANDIDATE_SIZE as f64).sqrt() as u32;
    let canvas_size = tile_size * grid_size;
    let canvas_img =
        image::RgbaImage::from_pixel(canvas_size, canvas_size, image::Rgba([255, 255, 255, 255]));
    let mut canvas = image::DynamicImage::ImageRgba8(canvas_img);

    let mut tasks = Vec::new();
    for c in candidates.iter().take(GRID_CANDIDATE_SIZE) {
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
        draw_tile_index_label(&mut canvas, index, tile_size, grid_size);
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
        system_prompt =
            "你是SKU同款鉴定器。必须严格匹配同一物理模具，宁可漏判也不能误判。只返回JSON。";
        user_prompt = format!(
            "图 A 是目标商品原图。图 B 是候选商品九宫格（编号 1 到 9）。\n\
            🚨 图 B 中只有前 {} 个格子有商品！\n\
            🚨 每个格子左上角有白底黑字编号，必须严格按图中编号返回，禁止用方位词推断编号。\n\
            🚨 商品名称参考：【{}】。\n\
            规则：\n\
            1. 只比较商品主体的物理结构/模具，忽略背景、文字、水印、角度。\n\
            2. 仅当核心结构、部件形态、连接方式都一致才算同款；拿不准必须排除。\n\
            3. 若是同款就返回编号；无同款返回空数组。\n\
            严格输出 JSON：\n\
            {{\n  \"reasoning\": \"简短结论\",\n  \"match_ids\": [1]\n}}",
            valid_count, name
        );
    } else {
        system_prompt =
            "你是SKU同款鉴定器。必须严格匹配同一物理模具，宁可漏判也不能误判。只返回JSON。";
        user_prompt = format!(
            "图 A 是目标商品原图。图 B 是候选商品九宫格（编号 1 到 9）。\n\
            🚨 图 B 中只有前 {} 个格子有商品！\n\
            🚨 每个格子左上角有白底黑字编号，必须严格按图中编号返回，禁止用方位词推断编号。\n\
            规则：\n\
            1. 只比较商品主体的物理结构/模具，忽略背景、文字、水印、角度。\n\
            2. 仅当核心结构、部件形态、连接方式都一致才算同款；拿不准必须排除。\n\
            3. 若是同款就返回编号；无同款返回空数组。\n\
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
    static CURRENCY_RE: OnceLock<Regex> = OnceLock::new();
    static YUAN_RE: OnceLock<Regex> = OnceLock::new();
    static PURE_RE: OnceLock<Regex> = OnceLock::new();

    fn parse_min(captures: &regex::Captures, first: usize, second: usize) -> Option<f64> {
        let first_value = captures
            .get(first)
            .and_then(|m| m.as_str().parse::<f64>().ok())
            .filter(|v| v.is_finite())?;
        let second_value = captures
            .get(second)
            .and_then(|m| m.as_str().parse::<f64>().ok())
            .filter(|v| v.is_finite());
        Some(second_value.map_or(first_value, |v| first_value.min(v)))
    }

    let normalized = price.replace([',', '，'], "");
    let currency_re = CURRENCY_RE.get_or_init(|| {
        Regex::new(r"[¥￥]\s*([0-9]+(?:\.[0-9]+)?)\s*(?:[-~至]\s*([0-9]+(?:\.[0-9]+)?))?").unwrap()
    });
    if let Some(cap) = currency_re.captures(&normalized) {
        return parse_min(&cap, 1, 2);
    }

    let yuan_re = YUAN_RE.get_or_init(|| {
        Regex::new(r"([0-9]+(?:\.[0-9]+)?)\s*(?:[-~至]\s*([0-9]+(?:\.[0-9]+)?))?\s*元").unwrap()
    });
    if let Some(cap) = yuan_re.captures(&normalized) {
        return parse_min(&cap, 1, 2);
    }

    let pure_re = PURE_RE.get_or_init(|| {
        Regex::new(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*(?:[-~至]\s*([0-9]+(?:\.[0-9]+)?))?\s*$").unwrap()
    });
    pure_re
        .captures(&normalized)
        .and_then(|cap| parse_min(&cap, 1, 2))
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
        .take(MAX_VERIFY_CANDIDATES)
        .collect::<Vec<_>>()
        .chunks(GRID_CANDIDATE_SIZE)
        .map(|chunk| chunk.to_vec())
        .collect()
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

fn summarize_matches(candidates: Vec<Candidate>) -> MatchSummary {
    if candidates.is_empty() {
        return MatchSummary::NoMatch;
    }

    let total_matches = candidates.len();
    match find_cheapest(candidates) {
        Some(cheapest) => MatchSummary::Cheapest(cheapest),
        None => MatchSummary::MatchedButPriceUnavailable { total_matches },
    }
}

fn prepare_final_review_candidates(candidates: Vec<Candidate>, limit: usize) -> Vec<Candidate> {
    if limit == 0 {
        return Vec::new();
    }

    let mut seen_urls = HashSet::new();
    let mut deduped = Vec::new();
    for candidate in candidates {
        if seen_urls.insert(candidate.item_url.clone()) {
            deduped.push(candidate);
        }
    }

    sort_candidates_by_price(&mut deduped);
    deduped
        .into_iter()
        .filter(|item| parse_positive_price_value(&item.price).is_some())
        .take(limit)
        .collect()
}

async fn verify_single_candidate(
    client: &Client,
    ozon_base64: &str,
    candidate: &Candidate,
    ozon_name_opt: Option<&str>,
) -> Option<bool> {
    let single_candidate_grid = create_grid_base64(client, std::slice::from_ref(candidate)).await?;
    let match_ids = verify_with_qwen_vl(
        client,
        ozon_base64,
        &single_candidate_grid,
        1,
        ozon_name_opt,
    )
    .await?;
    let normalized = normalize_match_ids(&match_ids, 1);
    Some(normalized.contains(&1))
}

async fn pick_cheapest_after_final_review(
    client: &Client,
    ozon_base64: &str,
    candidates: Vec<Candidate>,
    ozon_name_opt: Option<&str>,
) -> MatchSummary {
    let prepared =
        prepare_final_review_candidates(candidates.clone(), FINAL_REVIEW_CANDIDATE_LIMIT);
    if prepared.is_empty() {
        return summarize_matches(candidates);
    }

    println!(
        "🔎 启动终选复核：按价格前 {} 条逐条做一对一确认...",
        prepared.len()
    );

    let mut has_successful_review = false;
    for (index, candidate) in prepared.iter().enumerate() {
        match verify_single_candidate(client, ozon_base64, candidate, ozon_name_opt).await {
            Some(true) => {
                println!(
                    "✅ 终选复核通过：第 {} 个候选确认同款，价格 {}",
                    index + 1,
                    candidate.price
                );
                return MatchSummary::Cheapest(candidate.clone());
            }
            Some(false) => {
                has_successful_review = true;
                println!("⚠️ 终选复核排除第 {} 个候选。", index + 1);
            }
            None => {
                println!("⚠️ 终选复核请求失败，跳过第 {} 个候选。", index + 1);
            }
        }
    }

    if has_successful_review {
        MatchSummary::NoMatch
    } else {
        summarize_matches(candidates)
    }
}

async fn process_candidates(
    client: &Client,
    ozon_base64: &str,
    candidates: Vec<Candidate>,
    ozon_name_opt: Option<&str>,
) -> Result<MatchSummary, &'static str> {
    let chunks = build_verification_chunks(candidates);
    if chunks.is_empty() {
        return Ok(MatchSummary::NoMatch);
    }

    println!(
        "⚡ 并发启动 {} 个九宫格比对任务（最多 {} 组）...",
        chunks.len(),
        MAX_VERIFY_GROUPS
    );

    let ozon_base64_owned = ozon_base64.to_owned();
    let ozon_name_owned = ozon_name_opt.map(str::to_owned);
    let compare_tasks = chunks.into_iter().enumerate().map(|(chunk_index, chunk)| {
        let client = client.clone();
        let ozon_base64 = ozon_base64_owned.clone();
        let ozon_name = ozon_name_owned.clone();
        tokio::spawn(async move {
            let grid_base64 = create_grid_base64(&client, &chunk).await;
            let Some(grid_base64) = grid_base64 else {
                println!(
                    "⚠️ 第 {} 组九宫格生成失败（图片下载异常）。",
                    chunk_index + 1
                );
                return Ok(Vec::new());
            };

            let verify_result = verify_with_qwen_vl(
                &client,
                &ozon_base64,
                &grid_base64,
                chunk.len(),
                ozon_name.as_deref(),
            )
            .await;

            let Some(match_ids) = verify_result else {
                println!("⚠️ 第 {} 组大模型请求失败。", chunk_index + 1);
                return Err("大模型API调用异常/超时");
            };

            let matched_candidates = collect_matched_candidates(&chunk, &match_ids);
            println!(
                "📌 第 {} 组命中 {} 个高度相似候选。",
                chunk_index + 1,
                matched_candidates.len()
            );
            Ok(matched_candidates)
        })
    });

    let compare_results = join_all(compare_tasks).await;
    let mut merged_matches = Vec::new();
    let mut has_success_group = false;

    for task_result in compare_results {
        let result = match task_result {
            Ok(result) => result,
            Err(e) => {
                println!("⚠️ 并发任务异常终止: {}", e);
                continue;
            }
        };
        match result {
            Ok(mut group_matches) => {
                has_success_group = true;
                merged_matches.append(&mut group_matches);
            }
            Err(_) => {}
        }
    }

    if !has_success_group {
        return Err("大模型API调用异常/超时");
    }

    println!("✅ 并发比对完成，开始终选复核并按价格选择最低同款。");
    Ok(pick_cheapest_after_final_review(client, ozon_base64, merged_matches, ozon_name_opt).await)
}

// ==========================================
// 4. 调度总枢纽
// ==========================================
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
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
                        Ok(MatchSummary::Cheapest(cheapest)) => {
                            println!("✅ 第一重召回成功锁定最低价！");
                            final_cheapest = Some(cheapest);
                            final_status_msg = "AI比对成功(一次召回)".to_string();
                        }
                        Ok(MatchSummary::NoMatch) => {
                            println!(
                                "⚠️ 第一重视觉召回({}组并发)未命中同款，触发二次重绘！",
                                MAX_VERIFY_GROUPS
                            );
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
                                    Ok(MatchSummary::Cheapest(cheapest)) => {
                                        println!("🏆 绝杀！大模型利用『语义排错』，在二次全图中成功揪出真同款！");
                                        final_cheapest = Some(cheapest);
                                        final_status_msg = "AI比对成功(二次全图召回)".to_string();
                                    }
                                    Ok(MatchSummary::NoMatch) => {
                                        println!("❌ 两次召回均无果，确认为无同款。");
                                        final_status_msg = "无真实同款(两轮兜底)".to_string();
                                    }
                                    Ok(MatchSummary::MatchedButPriceUnavailable {
                                        total_matches,
                                    }) => {
                                        println!(
                                            "⚠️ 二次召回命中 {} 个同款候选，但价格字段不可解析。",
                                            total_matches
                                        );
                                        final_status_msg = "命中同款但价格不可解析".to_string();
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
                        Ok(MatchSummary::MatchedButPriceUnavailable { total_matches }) => {
                            println!(
                                "⚠️ 第一重召回命中 {} 个同款候选，但价格字段不可解析，触发二次重绘补价！",
                                total_matches
                            );

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
                                    Ok(MatchSummary::Cheapest(cheapest)) => {
                                        println!("🏆 绝杀！大模型利用『语义排错』，在二次全图中成功揪出真同款！");
                                        final_cheapest = Some(cheapest);
                                        final_status_msg = "AI比对成功(二次全图召回)".to_string();
                                    }
                                    Ok(MatchSummary::NoMatch) => {
                                        println!("⚠️ 二次召回未命中可确认同款。");
                                        final_status_msg =
                                            "命中同款但二次召回无可用报价".to_string();
                                    }
                                    Ok(MatchSummary::MatchedButPriceUnavailable {
                                        total_matches,
                                    }) => {
                                        println!(
                                            "⚠️ 两次召回均命中同款（第二轮 {} 条），但价格字段不可解析。",
                                            total_matches
                                        );
                                        final_status_msg = "命中同款但价格不可解析".to_string();
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

    fn candidate_with_price_and_url(price: &str, item_url: &str) -> Candidate {
        Candidate {
            title: "t".to_string(),
            price: price.to_string(),
            item_url: item_url.to_string(),
            image_url: "i".to_string(),
        }
    }

    #[test]
    fn parse_price_value_extracts_min_from_range() {
        let price = parse_price_value("¥12.5-18.0");
        assert_eq!(price, Some(12.5));
    }

    #[test]
    fn parse_price_value_ignores_moq_number_and_uses_currency_price() {
        let price = parse_price_value("2件起批 ¥19.80");
        assert_eq!(price, Some(19.8));
    }

    #[test]
    fn parse_price_value_returns_none_when_no_currency_price() {
        let price = parse_price_value("2件起批");
        assert_eq!(price, None);
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
    fn build_verification_chunks_orders_by_price_and_caps_to_36() {
        let mut candidates = Vec::new();
        for i in (1..=50).rev() {
            candidates.push(candidate_with_price(&format!("¥{}", i)));
        }

        let chunks = build_verification_chunks(candidates);
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].len(), 9);
        assert_eq!(chunks[1].len(), 9);
        assert_eq!(chunks[2].len(), 9);
        assert_eq!(chunks[3].len(), 9);
        assert_eq!(chunks[0][0].price, "¥1");
        assert_eq!(chunks[3][8].price, "¥36");
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

    #[test]
    fn summarize_matches_reports_no_match_on_empty() {
        let summary = summarize_matches(Vec::new());
        assert!(matches!(summary, MatchSummary::NoMatch));
    }

    #[test]
    fn summarize_matches_reports_cheapest_when_price_is_parseable() {
        let candidates = vec![
            candidate_with_price("面议"),
            candidate_with_price("¥6.5"),
            candidate_with_price("¥5.2"),
        ];

        let summary = summarize_matches(candidates);
        match summary {
            MatchSummary::Cheapest(candidate) => assert_eq!(candidate.price, "¥5.2"),
            _ => panic!("expected cheapest"),
        }
    }

    #[test]
    fn summarize_matches_reports_price_unavailable_when_only_non_numeric_prices() {
        let candidates = vec![candidate_with_price("面议"), candidate_with_price("待议")];

        let summary = summarize_matches(candidates);
        match summary {
            MatchSummary::MatchedButPriceUnavailable { total_matches } => {
                assert_eq!(total_matches, 2)
            }
            _ => panic!("expected MatchedButPriceUnavailable"),
        }
    }

    #[test]
    fn prepare_final_review_candidates_deduplicates_and_sorts_by_price() {
        let candidates = vec![
            candidate_with_price_and_url("¥3.5", "u1"),
            candidate_with_price_and_url("¥2.0", "u2"),
            candidate_with_price_and_url("¥1.5", "u2"),
            candidate_with_price_and_url("面议", "u3"),
            candidate_with_price_and_url("¥1.8", "u4"),
        ];

        let prepared = prepare_final_review_candidates(candidates, 10);
        assert_eq!(prepared.len(), 3);
        assert_eq!(prepared[0].price, "¥1.8");
        assert_eq!(prepared[1].price, "¥2.0");
        assert_eq!(prepared[2].price, "¥3.5");
    }

    #[test]
    fn prepare_final_review_candidates_respects_limit() {
        let candidates = vec![
            candidate_with_price_and_url("¥1.0", "u1"),
            candidate_with_price_and_url("¥2.0", "u2"),
            candidate_with_price_and_url("¥3.0", "u3"),
        ];

        let prepared = prepare_final_review_candidates(candidates, 2);
        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared[0].price, "¥1.0");
        assert_eq!(prepared[1].price, "¥2.0");
    }
}
