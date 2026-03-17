import os
import re
import json
import html
import hashlib
import datetime
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse
from rapidfuzz import fuzz

import feedparser
import requests
from dotenv import load_dotenv

load_dotenv("config.env")

GEMINI_API_KEY      = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL        = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()
FRESHRSS_URLS       = [u.strip() for u in os.getenv("FRESHRSS_URLS", "").split(",") if u.strip()]
MAX_ITEMS_PER_FEED  = int(os.getenv("MAX_ITEMS_PER_FEED", "200"))
DEDUP_THRESHOLD     = float(os.getenv("DEDUP_THRESHOLD", "0.78"))
OUTPUT_DIR          = os.getenv("OUTPUT_DIR", "output").strip()
SEND_TELEGRAM       = os.getenv("SEND_TELEGRAM", "false").lower() == "true"
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "").strip()
MAX_PUSH_ITEMS      = int(os.getenv("MAX_PUSH_ITEMS", "15"))
RECENT_HOURS        = int(os.getenv("RECENT_HOURS", "24"))
MIN_ARTICLE_SCORE   = int(os.getenv("MIN_ARTICLE_SCORE", "0"))
SENT_FILE           = "sent_articles.json"

# ── AI关键词表（LLM失败时兜底用）────────────────────────────────
AI_KEYWORDS = [
    "ai", "llm", "gpt", "claude", "openai", "anthropic", "deepmind",
    "machine learning", "neural network", "transformer",
    "foundation model", "generative ai",
]

# ── 公司关键词表（LLM失败时兜底用）──────────────────────────────
COMPANY_KEYWORDS = {
    "Shopee":       ["shopee", "sea limited", "sea group", "garena"],
    "TikTok Shop":  ["tiktok shop", "tiktok e-commerce", "tiktok commerce"],
    "Tencent":      ["tencent", "wechat", "weixin", "video accounts", "hunyuan"],
    "Coupang":      ["coupang", "rocket delivery"],
    "MercadoLibre": ["mercadolibre", "meli", "mercado libre"],
    "Amazon":       ["amazon", "aws"],
    "AppLovin":     ["applovin"],
    "Temu":         ["temu", "pinduoduo", "pdd"],
}

# ── 关键词评分（LLM失败时兜底用）────────────────────────────────
HIGH_SIGNAL_WORDS = [
    "earnings", "regulation", "antitrust", "acquisition",
    "ipo", "policy", "investment", "lawsuit", "ban", "fine",
]
MID_SIGNAL_WORDS = [
    "growth", "launch", "strategy", "expansion", "partnership",
    "revenue", "profit", "market share",
]


def domain_from_url(url: str) -> str | None:
    try:
        host = urlparse(url).netloc
        host = re.sub(r"^www\d?\.", "", host)
        return host or None
    except Exception:
        return None


def now_str() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs("logs", exist_ok=True)


# ── Sent history ────────────────────────────────────────────────

def load_sent() -> dict:
    if not os.path.exists(SENT_FILE):
        return {"links": [], "title_hashes": []}
    try:
        with open(SENT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"links": [], "title_hashes": []}
        data.setdefault("links", [])
        data.setdefault("title_hashes", [])
        return data
    except Exception:
        return {"links": [], "title_hashes": []}


def save_sent(sent: dict):
    sent["links"]        = sent.get("links", [])[-5000:]
    sent["title_hashes"] = sent.get("title_hashes", [])[-5000:]
    with open(SENT_FILE, "w", encoding="utf-8") as f:
        json.dump(sent, f, ensure_ascii=False, indent=2)


# ── Title utils ─────────────────────────────────────────────────

def normalize_title(title: str) -> str:
    t = html.unescape((title or "").lower())
    t = re.sub(r"\s+", " ", t)
    t = re.sub(
        r"[-–|]\s*(yahoo finance|msn|marketbeat|seeking alpha|tipranks|finviz|"
        r"zacks investment research|simplywall\.st|the information)$",
        "", t, flags=re.IGNORECASE,
    )
    t = re.sub(r"[^a-z0-9\u4e00-\u9fff ]", "", t)
    return t.strip()


def title_hash(title: str) -> str:
    return hashlib.md5(normalize_title(title).encode("utf-8")).hexdigest()


# ── Published time ───────────────────────────────────────────────

def parse_published(entry) -> datetime.datetime | None:
    candidates = [
        getattr(entry, "published", None),
        getattr(entry, "updated",   None),
        getattr(entry, "pubDate",   None),
    ]
    for c in candidates:
        if not c:
            continue
        try:
            dt = parsedate_to_datetime(c)
            if dt.tzinfo:
                dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            pass
        try:
            dt = datetime.datetime.fromisoformat(c.replace("Z", "+00:00"))
            if dt.tzinfo:
                dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            pass
    return None


def is_recent(pub_dt: datetime.datetime | None, hours: int = 24) -> bool:
    if pub_dt is None:
        return True
    now_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    return (now_utc - pub_dt).total_seconds() <= hours * 3600


# ── Fetch ────────────────────────────────────────────────────────

def fetch_one_feed(url: str) -> list[dict]:
    print(f"[{now_str()}] Fetching: {url}")
    resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    feed = feedparser.parse(resp.text)
    if getattr(feed, "bozo", False):
        print(f"[WARN] RSS parse warning: {getattr(feed, 'bozo_exception', None)}")

    items = []
    for e in getattr(feed, "entries", [])[:MAX_ITEMS_PER_FEED]:
        raw     = getattr(e, "summary", "") or getattr(e, "description", "")
        summary = html.unescape(re.sub(r"<[^>]+>", "", raw).strip())[:500]
        source  = None
        if hasattr(e, "source"):
            try:
                source = e.source.get("title")
            except Exception:
                pass
        if not source:
            source = domain_from_url(getattr(e, "link", "") or "")
        if not source:
            source = "未知来源"
        pub_dt = parse_published(e)
        item = {
            "title":         (getattr(e, "title", "") or "").strip() or "(无标题)",
            "link":          (getattr(e, "link",  "") or "").strip(),
            "source":        source,
            "summary":       summary or "（无摘要）",
            "published_dt":  pub_dt,
            "published_str": pub_dt.strftime("%Y-%m-%d %H:%M") if pub_dt else "",
        }
        item["title_hash"] = title_hash(item["title"])
        items.append(item)

    print(f"[{now_str()}] Fetched {len(items)} items")
    return items


def fetch_all_feeds(urls: list[str]) -> list[dict]:
    all_articles = []
    for url in urls:
        try:
            all_articles.extend(fetch_one_feed(url))
        except Exception as e:
            print(f"[ERROR] Failed to fetch {url}: {e}")
    print(f"[{now_str()}] Total fetched: {len(all_articles)}")
    return all_articles


# ── Filters ─────────────────────────────────────────────────────

def filter_recent(articles: list[dict], hours: int = 24) -> list[dict]:
    result = [a for a in articles if is_recent(a.get("published_dt"), hours)]
    print(f"[{now_str()}] Recent ({hours}h): {len(result)}")
    return result


def _title_sim(a: str, b: str) -> float:
    return max(fuzz.token_sort_ratio(a, b), fuzz.partial_ratio(a, b))


def dedupe(articles: list[dict], threshold: float = 0.78) -> list[dict]:
    seen_links, seen_titles, result = set(), [], []
    for a in articles:
        link = a.get("link", "").strip()
        nt   = normalize_title(a.get("title", ""))
        if link and link in seen_links:
            continue
        if any(_title_sim(nt, s) >= threshold * 100 for s in seen_titles):
            continue
        if link:
            seen_links.add(link)
        seen_titles.append(nt)
        result.append(a)
    print(f"[{now_str()}] After dedupe: {len(result)}")
    return result


def filter_already_sent(articles: list[dict], sent_store: dict) -> list[dict]:
    sent_links  = set(sent_store.get("links", []))
    sent_hashes = set(sent_store.get("title_hashes", []))
    result = [
        a for a in articles
        if a["link"] not in sent_links and a["title_hash"] not in sent_hashes
    ]
    print(f"[{now_str()}] After sent-filter: {len(result)}")
    return result


def update_sent_store(sent_store: dict, articles: list[dict]) -> dict:
    links        = sent_store.get("links", [])
    title_hashes = sent_store.get("title_hashes", [])
    for a in articles:
        if a["link"]:
            links.append(a["link"])
        title_hashes.append(a["title_hash"])
    sent_store["links"]        = links
    sent_store["title_hashes"] = title_hashes
    return sent_store


# ── Tagging (keyword fallback) ───────────────────────────────────

def detect_company(article: dict) -> str | None:
    text = (article.get("title", "") + " " + article.get("summary", "")).lower()
    for company, keywords in COMPANY_KEYWORDS.items():
        if any(k in text for k in keywords):
            return company
    return None


def detect_ai(article: dict) -> bool:
    text = (article.get("title", "") + " " + article.get("summary", "")).lower()
    return any(k in text for k in AI_KEYWORDS)


def _keyword_tag(article: dict):
    """关键词匹配兜底打标（LLM失败时使用）。"""
    company = detect_company(article)
    if company:
        article["category"]    = "company"
        article["company_tag"] = company
    elif detect_ai(article):
        article["category"]    = "ai"
        article["company_tag"] = "AI技术"
    else:
        article["category"]    = "other"
        article["company_tag"] = "市场动态"


def _keyword_score(article: dict) -> int:
    """关键词评分兜底，归一化到 1-10（LLM失败时使用）。"""
    text = (article.get("title", "") + " " + article.get("summary", "")).lower()
    score = 0
    for w in HIGH_SIGNAL_WORDS:
        if w in text:
            score += 3
    for w in MID_SIGNAL_WORDS:
        if w in text:
            score += 1
    if article.get("company_tag", "市场动态") != "市场动态":
        score += 5
    return min(10, max(1, score))


# ── v7: LLM tagging + ranking ────────────────────────────────────

def llm_tag_and_rank(articles: list[dict], limit: int | None = None) -> list[dict]:
    """
    用一次 Gemini 调用完成分类（company/ai/other）+ 重要性评分（1-10）。
    在 dedup+filter 之后调用，输入约 30-80 条，token 消耗极低。
    LLM 失败时自动回退到关键词匹配。
    """
    limit = limit or max(MAX_PUSH_ITEMS * 2, 20)
    companies = list(COMPANY_KEYWORDS.keys())

    # 只取标题 + 摘要前120字，节省 token
    lines = []
    for i, a in enumerate(articles):
        lines.append(f"{i}. {a['title']}\n   {a['summary'][:120]}")

    prompt = f"""你是一名科技行业研究员。请对以下新闻分类并评分重要性。

分类规则：
- category="company"：涉及以下公司之一（填入company字段）：{", ".join(companies)}
- category="ai"：涉及AI/LLM/机器学习/大模型/生成式AI等，company填"AI技术"
- category="other"：其他新闻，company填"市场动态"

重要性评分（1-10）：
- 8-10：财报/监管/反垄断/收购/IPO/重大产品发布/CEO变动
- 5-7：战略合作/市场扩张/新功能/行业报告
- 1-4：普通资讯/观点文章/市场数据

只输出JSON数组，不要任何解释：
[{{"id":0,"category":"company","company":"Amazon","importance":8}},...]

新闻：
{chr(10).join(lines)}
"""

    try:
        text = call_gemini(prompt)
        match = re.search(r"\[.*\]", text, re.S)
        if not match:
            raise ValueError("no JSON array")
        tagged = json.loads(match.group(0))

        for t in tagged:
            idx = t.get("id")
            if idx is None or not (0 <= idx < len(articles)):
                continue
            articles[idx]["category"]    = t.get("category", "other")
            articles[idx]["company_tag"] = t.get("company", "市场动态")
            articles[idx]["score"]       = min(10, max(1, int(t.get("importance", 5))))

        # LLM 遗漏的条目用关键词兜底
        for a in articles:
            if "category" not in a:
                _keyword_tag(a)
                a["score"] = _keyword_score(a)

    except Exception as e:
        print(f"[WARN] LLM tagging failed ({e}), falling back to keyword tagging")
        for a in articles:
            _keyword_tag(a)
            a["score"] = _keyword_score(a)

    company_count = sum(1 for a in articles if a.get("category") == "company")
    ai_count      = sum(1 for a in articles if a.get("category") == "ai")
    print(f"[{now_str()}] LLM tagged — Company:{company_count} | AI:{ai_count} | Total:{len(articles)}")

    articles.sort(key=lambda x: x.get("score", 0), reverse=True)
    selected = articles[:limit]
    print(f"[{now_str()}] After LLM ranking: top {len(selected)} (limit={limit})")
    return selected


# ── Gemini ──────────────────────────────────────────────────────

def call_gemini(prompt: str) -> str:
    if not GEMINI_API_KEY:
        raise EnvironmentError("请在 config.env 中设置 GEMINI_API_KEY")
    url = (
        "https://generativelanguage.googleapis.com/v1beta/"
        f"models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    )
    resp = requests.post(
        url,
        headers={"Content-Type": "application/json"},
        json={"contents": [{"parts": [{"text": prompt}]}]},
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()
    try:
        return data["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception:
        raise RuntimeError("Gemini 响应解析失败：\n" + json.dumps(data, ensure_ascii=False, indent=2))


def build_company_grouped_headlines(articles: list[dict], ai_mode: bool = False) -> list[dict]:
    """
    公司分组由 Python 完成（基于 company_tag），Gemini 只负责写 headline。
    ai_mode=True: 使用 AI技术分析师视角撰写 headline。
    返回 [{"company": "Shopee", "items": [{"headline":..., "source":..., "link":...}]}]
    """
    lines = []
    for i, a in enumerate(articles, 1):
        lines.append(
            f"{i}. [分类: {a.get('company_tag', '市场动态')}]\n"
            f"   标题: {a['title']}\n"
            f"   摘要: {a['summary']}\n"
            f"   来源: {a['source']}\n"
            f"   链接: {a['link']}"
        )

    json_example = """[
  {
    "company": "AI技术",
    "items": [
      {
        "headline": "Anthropic发布Claude新模型，强调安全对齐能力提升与多模态推理突破",
        "source": "Anthropic Blog",
        "link": "https://example.com/1"
      }
    ]
  }
]"""

    if ai_mode:
        prompt = f"""你是一名AI技术分析师。请把下面的AI/技术新闻改写为精炼的中文投研 headline。

要求：
1. 每条新闻改写成一句话，包含：文章核心观点 + 关键技术概念 + 行业影响
2. 分类已在"[分类: ...]"字段中标注，按此分组输出，不要改变分组
3. 每条新闻保留 source 和 link
4. 语言简洁清晰，适合投资研究阅读
5. 只输出 JSON，不要任何解释

JSON 格式：
{json_example}

新闻列表：
{chr(10).join(lines)}
"""
    else:
        prompt = f"""你是一名中文投研新闻编辑。请把下面的新闻改写为精炼的中文投研 headline。

要求：
1. 每条新闻改写成一句话：事件 + 可能的影响
2. 分类已在"[分类: ...]"字段中标注，按此分组输出，不要改变分组
3. 每条新闻保留 source 和 link
4. 只输出 JSON，不要任何解释

JSON 格式：
[
  {{
    "company": "Amazon",
    "items": [
      {{
        "headline": "Amazon获临时禁令阻止Perplexity AI购物代理访问，显示其加强数据护城河",
        "source": "MarketBeat",
        "link": "https://example.com/1"
      }}
    ]
  }}
]

新闻列表：
{chr(10).join(lines)}
"""

    text = call_gemini(prompt)

    try:
        match = re.search(r"\[.*\]", text, re.S)
        if not match:
            raise ValueError("Gemini 未返回 JSON 数组")
        return json.loads(match.group(0))
    except Exception as e:
        raise RuntimeError(f"Headline 生成失败: {e}\n原始返回:\n{text}")


def build_brief(articles: list[dict]) -> str:
    """仅基于关注公司文章生成投研 brief。"""
    lines = [
        f"{i}. [公司: {a.get('company_tag', '未知')}] {a['title']}\n"
        f"   摘要: {a['summary']}\n"
        f"   来源: {a['source']}"
        for i, a in enumerate(articles, 1)
    ]
    prompt = f"""你是一名中文投研分析师。请基于以下新闻，生成一份中文投研 brief。

输出结构：
1. 今日核心变化（3-5条）
2. 重要公司/板块影响
3. 关键数据点与待验证项
4. 潜在投资含义（短期 / 中期）
5. 风险与不确定性

要求：明确事实与推断边界，控制在 400-700 字，不附原文链接。

新闻列表：
{chr(10).join(lines)}
"""
    return call_gemini(prompt)


# ── Save Markdown ────────────────────────────────────────────────

def save_markdown_digest(grouped: list[dict], raw_articles: list[dict], label: str = "news-digest") -> str:
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    ts    = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")
    path  = os.path.join(OUTPUT_DIR, f"{ts}-{label}.md")
    title = "AI技术速递" if label == "ai-digest" else "新闻速递"

    lines = [
        "---", f"date: {today}", "tags:", "  - auto-generated", f"  - {label}",
        f"sources: {len(raw_articles)} articles", "---", "",
        f"# {title} {today}", "",
    ]
    for block in grouped:
        company = block.get("company", "未分类")
        items   = block.get("items", [])
        if not items:
            continue
        lines.append(f"## {company}")
        for item in items:
            hl     = item.get("headline", "").strip()
            source = item.get("source", "未知来源").strip()
            link   = item.get("link", "").strip()
            lines.append(f"- {hl}（[{source}]({link})）" if link else f"- {hl}（{source}）")
        lines.append("")

    lines += ["---", "", "## Source Appendix", ""]
    for a in raw_articles:
        tag = a.get("company_tag", "")
        lines.append(f"- [{a['title']}]({a['link']}) — {a['source']}" + (f" `{tag}`" if tag else ""))

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[{now_str()}] Saved digest: {path}")
    return path


def save_markdown_brief(brief_text: str, raw_articles: list[dict]) -> str:
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    ts    = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")
    path  = os.path.join(OUTPUT_DIR, f"{ts}-investment-brief.md")

    lines = [
        "---", f"date: {today}", "tags:", "  - auto-generated", "  - investment-brief",
        f"sources: {len(raw_articles)} articles", "---", "",
        f"# 投研 Brief {today}", "", brief_text, "", "---", "", "## Source Appendix", "",
    ]
    for a in raw_articles:
        tag = a.get("company_tag", "")
        lines.append(f"- [{a['title']}]({a['link']}) — {a['source']}" + (f" `{tag}`" if tag else ""))

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[{now_str()}] Saved brief: {path}")
    return path


# ── Telegram ─────────────────────────────────────────────────────

def escape_html(text: str) -> str:
    return html.escape(text or "", quote=True)


def build_telegram_message(grouped: list[dict]) -> str:
    lines = []
    total = 0
    for block in grouped:
        if total >= MAX_PUSH_ITEMS:
            break
        company = escape_html(block.get("company", "未分类"))
        items   = block.get("items", [])
        if not items:
            continue
        lines.append(f"<b>{company}</b>")
        for item in items:
            if total >= MAX_PUSH_ITEMS:
                break
            hl     = escape_html(item.get("headline", "").strip())
            source = escape_html(item.get("source", "未知来源").strip())
            link   = item.get("link", "").strip()
            link_escaped = html.escape(link, quote=True)
            lines.append(
                f"• {hl}（<a href=\"{link_escaped}\">{source}</a>）" if link
                else f"• {hl}（{source}）"
            )
            total += 1
        lines.append("")
    return "\n".join(lines).strip() if total else ""


def send_telegram(text: str):
    if not SEND_TELEGRAM:
        print("Telegram disabled.")
        return
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram config missing.")
        return
    if not text.strip():
        print("Telegram text empty.")
        return
    resp = requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={
            "chat_id":                  TELEGRAM_CHAT_ID,
            "text":                     text[:4000],
            "parse_mode":               "HTML",
            "disable_web_page_preview": True,
        },
        timeout=30,
    )
    resp.raise_for_status()
    print(f"[{now_str()}] Telegram sent.")


def send_two_messages(company_grouped: list[dict], ai_grouped: list[dict]):
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    company_text = build_telegram_message(company_grouped)
    ai_text      = build_telegram_message(ai_grouped)
    if company_text:
        send_telegram(f"📊 <b>公司情报</b> {today}\n\n" + company_text)
    if ai_text:
        send_telegram(f"🤖 <b>AI技术</b> {today}\n\n" + ai_text)


# ── Main ─────────────────────────────────────────────────────────

def main():
    ensure_dirs()
    if not FRESHRSS_URLS:
        raise EnvironmentError("请在 config.env 中设置 FRESHRSS_URLS")

    sent_store = load_sent()

    articles = fetch_all_feeds(FRESHRSS_URLS)
    articles = filter_recent(articles, hours=RECENT_HOURS)
    articles = dedupe(articles, threshold=DEDUP_THRESHOLD)
    articles_new = filter_already_sent(articles, sent_store)

    if not articles_new:
        print(f"[{now_str()}] No new articles to push.")
        return

    # v7: LLM tagging + ranking in one call (runs on deduped set, ~30-80 articles)
    ranked = llm_tag_and_rank(articles_new)

    # Optional quality gate (importance 1-10, default disabled)
    if MIN_ARTICLE_SCORE > 0:
        ranked = [a for a in ranked if a.get("score", 0) >= MIN_ARTICLE_SCORE]
        print(f"[{now_str()}] After score filter (>={MIN_ARTICLE_SCORE}): {len(ranked)}")

    if not ranked:
        print(f"[{now_str()}] No articles passed quality gate, skipping.")
        return

    sent_store = update_sent_store(sent_store, ranked)
    save_sent(sent_store)

    company_articles = [a for a in ranked if a["category"] == "company"]
    ai_articles      = [a for a in ranked if a["category"] == "ai"]

    digest_files    = []
    company_grouped = []
    ai_grouped      = []

    if company_articles:
        company_grouped = build_company_grouped_headlines(company_articles)
        digest_files.append(save_markdown_digest(company_grouped, company_articles, label="company-digest"))

    if ai_articles:
        ai_grouped = build_company_grouped_headlines(ai_articles, ai_mode=True)
        digest_files.append(save_markdown_digest(ai_grouped, ai_articles, label="ai-digest"))

    brief_file = None
    if company_articles:
        brief_text = build_brief(company_articles)
        brief_file = save_markdown_brief(brief_text, company_articles)

    send_two_messages(company_grouped, ai_grouped)

    summary = " | ".join(digest_files) if digest_files else "(no digest)"
    if brief_file:
        summary += f" | Brief: {brief_file}"
    print(f"[{now_str()}] Done. {summary}")


if __name__ == "__main__":
    main()
