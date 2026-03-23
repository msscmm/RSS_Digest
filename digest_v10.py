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

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(_BASE_DIR)
load_dotenv(os.path.join(_BASE_DIR, "config.env"))

GEMINI_API_KEY       = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL         = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()
FRESHRSS_URLS        = [u.strip() for u in os.getenv("FRESHRSS_URLS", "").split(",") if u.strip()]
MAX_ITEMS_PER_FEED   = int(os.getenv("MAX_ITEMS_PER_FEED", "200"))
DEDUP_THRESHOLD      = float(os.getenv("DEDUP_THRESHOLD", "0.78"))
OUTPUT_DIR           = os.getenv("OUTPUT_DIR", "output").strip()
SEND_TELEGRAM        = os.getenv("SEND_TELEGRAM", "false").lower() == "true"
TELEGRAM_BOT_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "").strip()
MAX_PUSH_ITEMS       = int(os.getenv("MAX_PUSH_ITEMS", "15"))
RECENT_HOURS         = int(os.getenv("RECENT_HOURS", "24"))
MIN_ARTICLE_SCORE    = int(os.getenv("MIN_ARTICLE_SCORE", "0"))
SENT_FILE            = os.getenv("SENT_FILE", "sent_articles.json").strip()
SENT_RETENTION_HOURS = int(os.getenv("SENT_RETENTION_HOURS", "72"))
LLM_RANK_LIMIT       = int(os.getenv("LLM_RANK_LIMIT", str(max(MAX_PUSH_ITEMS * 4, 60))))
MIN_COMPANY_ITEMS    = int(os.getenv("MIN_COMPANY_ITEMS", "8"))
MIN_AI_ITEMS         = int(os.getenv("MIN_AI_ITEMS", "8"))
MIN_BLOG_ITEMS       = int(os.getenv("MIN_BLOG_ITEMS", "5"))
HTTP_TIMEOUT         = int(os.getenv("HTTP_TIMEOUT", "25"))
TELEGRAM_MAX_CHARS   = int(os.getenv("TELEGRAM_MAX_CHARS", "3900"))

# v10 新增常量
MAX_BLOG_PRE_FILTER      = int(os.getenv("MAX_BLOG_PRE_FILTER", "40"))
TOP_FULLTEXT             = int(os.getenv("TOP_FULLTEXT", "30"))
RAW_FEED_LIMIT           = int(os.getenv("RAW_FEED_LIMIT", "50"))
TELEGRAM_RAW_BOT_TOKEN   = os.getenv("TELEGRAM_RAW_BOT_TOKEN", "").strip()
TELEGRAM_RAW_CHAT_ID     = os.getenv("TELEGRAM_RAW_CHAT_ID", "").strip()

AI_KEYWORDS = [
    "ai", "llm", "gpt", "claude", "openai", "anthropic", "deepmind",
    "machine learning", "neural network", "transformer", "foundation model",
    "generative ai", "multimodal", "reasoning model", "agent", "agents",
]

COMPANY_KEYWORDS = {
    "Shopee":       ["shopee", "sea limited", "sea group", "garena"],
    "TikTok Shop":  ["tiktok shop", "tiktok e-commerce", "tiktok commerce"],
    "Tencent":      ["tencent", "wechat", "weixin", "video accounts", "hunyuan"],
    "Coupang":      ["coupang", "rocket delivery"],
    "MercadoLibre": ["mercadolibre", "meli", "mercado libre"],
    "Amazon":       ["amazon", "aws", "prime video", "prime air"],
    "AppLovin":     ["applovin"],
    "Temu":         ["temu", "pinduoduo", "pdd"],
    "Meta":         ["meta", "facebook", "instagram", "whatsapp"],
    "Google":       ["google", "alphabet", "gemini", "deepmind", "youtube"],
    "Microsoft":    ["microsoft", "openai partnership", "copilot", "azure ai"],
    "Anthropic":    ["anthropic", "claude"],
}

# v9: 两层公司分桶 — 核心公司优先推送，Global Tech 单独成栏
FOCUS_COMPANIES = {
    "Shopee", "TikTok Shop", "Tencent", "Coupang", "MercadoLibre", "AppLovin",
}
GLOBAL_TECH = {
    "Meta", "Google", "Microsoft", "Amazon", "Anthropic", "Temu",
}

HIGH_SIGNAL_WORDS = [
    "earnings", "regulation", "antitrust", "acquisition", "ipo", "policy",
    "investment", "lawsuit", "ban", "fine", "guidance", "sec", "doj",
    "probe", "investigation", "results", "quarter", "margin",
]
MID_SIGNAL_WORDS = [
    "growth", "launch", "strategy", "expansion", "partnership", "revenue",
    "profit", "market share", "pricing", "adoption", "roadmap", "rollout",
    "deployment", "monetization",
]

SOURCE_MAP = {
    "engadget.com": "Engadget",
    "gizmodo.com": "Gizmodo",
    "mashable.com": "Mashable",
    "techcrunch.com": "TechCrunch",
    "theverge.com": "The Verge",
    "wired.com": "Wired",
    "arstechnica.com": "Ars Technica",
    "bloomberg.com": "Bloomberg",
    "reuters.com": "Reuters",
    "ft.com": "Financial Times",
    "wsj.com": "WSJ",
    "cnbc.com": "CNBC",
    "fortune.com": "Fortune",
    "seekingalpha.com": "Seeking Alpha",
    "marketwatch.com": "MarketWatch",
    "yahoo.com": "Yahoo",
    "yahoo.com/news": "Yahoo",
    "substack.com": "Substack",
    "anthropic.com": "Anthropic",
    "openai.com": "OpenAI",
    "ai.google.dev": "Google AI",
    "blog.google": "Google Blog",
    "blog.google/products": "Google Blog",
    "microsoft.com": "Microsoft",
    "blogs.microsoft.com": "Microsoft Blog",
    "devblogs.microsoft.com": "Microsoft DevBlogs",
    "aws.amazon.com": "AWS",
    "aboutamazon.com": "Amazon",
    "newsroom.tiktok.com": "TikTok Newsroom",
    "engineering.fb.com": "Meta Engineering",
}

TOP_TIER_DOMAINS = {
    "reuters.com", "bloomberg.com", "ft.com", "wsj.com", "cnbc.com",
    "techcrunch.com", "theverge.com", "wired.com", "arstechnica.com",
    "engadget.com", "gizmodo.com", "mashable.com",
}

BLOG_HINTS = [
    "substack", "blog", "newsletter", "medium.com", "ghost.io", "github.io",
    "personal", "indie", "thoughts", "essays",
]

KNOWN_PERSONAL_BLOGS = {
    "simonwillison.net", "antirez.com", "joanwestenberg.com", "pluralistic.net",
    "garymarcus.substack.com", "dfarq.homeip.net", "geohot.github.io",
    "martinalderson.com", "nesbitt.io", "shkspr.mobi", "lnotes.dragas.net",
}


# ── Helpers ─────────────────────────────────────────────────────

def now_local() -> datetime.datetime:
    return datetime.datetime.now()


def now_utc_naive() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


def now_str() -> str:
    return now_local().strftime("%Y-%m-%d %H:%M:%S")


def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs("logs", exist_ok=True)


def domain_from_url(url: str) -> str | None:
    try:
        host = urlparse(url).netloc.lower().strip()
        host = re.sub(r"^www\d?\.", "", host)
        return host or None
    except Exception:
        return None


def base_domain(host: str | None) -> str | None:
    if not host:
        return None
    parts = host.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def prettify_domain(host: str | None) -> str:
    if not host:
        return "未知来源"
    mapped = SOURCE_MAP.get(host) or SOURCE_MAP.get(base_domain(host) or "")
    if mapped:
        return mapped
    name = host.replace(".com", "").replace(".net", "").replace(".org", "")
    name = name.replace(".io", "").replace(".co", "")
    name = name.replace("-", " ").replace("_", " ")
    return " ".join(w.capitalize() for w in name.split()) or host


def clean_text(s: str, limit: int | None = None) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if limit and len(s) > limit:
        return s[:limit].rstrip() + "…"
    return s


# ── Sent history with TTL ───────────────────────────────────────

def _legacy_sent_to_dict(data) -> dict:
    if isinstance(data, dict):
        return data
    return {"records": []}


def load_sent() -> dict:
    if not os.path.exists(SENT_FILE):
        return {"records": []}
    try:
        with open(SENT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        data = _legacy_sent_to_dict(data)
        records = data.get("records", [])

        # Backward compatibility: old format {links:[], title_hashes:[]}
        if not records and ("links" in data or "title_hashes" in data):
            created_at = now_utc_naive().isoformat()
            for link in data.get("links", []):
                records.append({"type": "link", "value": link, "created_at": created_at})
            for h in data.get("title_hashes", []):
                records.append({"type": "title_hash", "value": h, "created_at": created_at})
        data["records"] = records
        return prune_sent_records(data)
    except Exception:
        return {"records": []}


def prune_sent_records(sent: dict) -> dict:
    cutoff = now_utc_naive() - datetime.timedelta(hours=SENT_RETENTION_HOURS)
    kept = []
    for r in sent.get("records", []):
        try:
            created_at = datetime.datetime.fromisoformat(r.get("created_at", ""))
            if created_at >= cutoff:
                kept.append(r)
        except Exception:
            continue
    sent["records"] = kept[-10000:]
    return sent


def save_sent(sent: dict):
    sent = prune_sent_records(sent)
    with open(SENT_FILE, "w", encoding="utf-8") as f:
        json.dump(sent, f, ensure_ascii=False, indent=2)


def sent_lookup(sent: dict) -> tuple[set[str], set[str]]:
    links, hashes = set(), set()
    for r in sent.get("records", []):
        if r.get("type") == "link":
            links.add(r.get("value", ""))
        elif r.get("type") == "title_hash":
            hashes.add(r.get("value", ""))
    return links, hashes


def update_sent_store(sent: dict, articles: list[dict]) -> dict:
    created_at = now_utc_naive().isoformat()
    records = sent.get("records", [])
    for a in articles:
        if a.get("link"):
            records.append({"type": "link", "value": a["link"], "created_at": created_at})
        if a.get("title_hash"):
            records.append({"type": "title_hash", "value": a["title_hash"], "created_at": created_at})
    sent["records"] = records
    return prune_sent_records(sent)


# ── Title / time utils ──────────────────────────────────────────

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


def parse_published(entry) -> datetime.datetime | None:
    candidates = [
        getattr(entry, "published", None),
        getattr(entry, "updated", None),
        getattr(entry, "pubDate", None),
        getattr(entry, "created", None),
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
            dt = datetime.datetime.fromisoformat(str(c).replace("Z", "+00:00"))
            if dt.tzinfo:
                dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            pass
    return None


def is_recent(pub_dt: datetime.datetime | None, hours: int = 24) -> bool:
    # No timestamp: trust FreshRSS's own hours=168 pre-filter as the safety net.
    # Many Asian/Chinese feeds use non-standard date formats that parse_published()
    # cannot handle, which would incorrectly drop Tencent/Shopee/Coupang articles.
    if pub_dt is None:
        return True
    return (now_utc_naive() - pub_dt).total_seconds() <= hours * 3600


# ── Feed fetch ──────────────────────────────────────────────────

def classify_source_type(feed_title: str, source_name: str, domain: str | None) -> str:
    text = f"{feed_title} {source_name} {domain or ''}".lower()
    if any(h in text for h in BLOG_HINTS):
        return "blog"
    bd = base_domain(domain) if domain else None
    if domain in TOP_TIER_DOMAINS or bd in TOP_TIER_DOMAINS:
        return "media"
    if domain and any(x in domain for x in ["substack", "medium", "ghost.io", "github.io"]):
        return "blog"
    if domain in KNOWN_PERSONAL_BLOGS or bd in KNOWN_PERSONAL_BLOGS:
        return "blog"
    return "other"


def fetch_one_feed(url: str) -> list[dict]:
    print(f"[{now_str()}] Fetching: {url}")
    resp = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    feed = feedparser.parse(resp.text)
    if getattr(feed, "bozo", False):
        print(f"[WARN] RSS parse warning: {getattr(feed, 'bozo_exception', None)}")

    feed_title = clean_text(getattr(feed.feed, "title", ""), 120)
    items = []
    for e in getattr(feed, "entries", [])[:MAX_ITEMS_PER_FEED]:
        link = (getattr(e, "link", "") or "").strip()
        domain = domain_from_url(link)

        source = None
        if hasattr(e, "source"):
            try:
                source = clean_text(e.source.get("title") or "")
            except Exception:
                source = None
        if not source:
            source = feed_title or prettify_domain(domain)
        if not source:
            source = "未知来源"

        raw = getattr(e, "summary", "") or getattr(e, "description", "") or getattr(e, "content", "")
        summary = clean_text(str(raw), 500) or "（无摘要）"
        pub_dt = parse_published(e)

        item = {
            "title": clean_text(getattr(e, "title", "") or "(无标题)", 300),
            "link": link,
            "domain": domain,
            "feed_title": feed_title,
            "source": source,
            "source_display": prettify_domain(domain) if domain else source,
            "source_type": classify_source_type(feed_title, source, domain),
            "summary": summary,
            "published_dt": pub_dt,
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
        nt = normalize_title(a.get("title", ""))
        if not nt:
            continue
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
    sent_links, sent_hashes = sent_lookup(sent_store)
    result = [
        a for a in articles
        if a.get("link") not in sent_links and a.get("title_hash") not in sent_hashes
    ]
    print(f"[{now_str()}] After sent-filter: {len(result)}")
    return result


# v10: 快速预过滤，防止博客文章挤占 LLM ranking 配额
def fast_filter(articles: list[dict]) -> list[dict]:
    core = []   # company + AI + media（全保）
    blogs = []  # blog（限量）
    for a in articles:
        text = (a.get("title", "") + " " + a.get("summary", "")).lower()
        if detect_company(a) or any(k in text for k in AI_KEYWORDS) \
                or a.get("source_type") == "media":
            core.append(a)
        elif a.get("source_type") == "blog":
            blogs.append(a)
        # source_type=="other" 且不含公司/AI关键词：丢弃
    blogs.sort(key=lambda x: x.get("published_dt") or datetime.datetime.min, reverse=True)
    result = core + blogs[:MAX_BLOG_PRE_FILTER]
    print(f"[{now_str()}] [FAST FILTER] {len(articles)} → {len(result)} (core={len(core)}, blog≤{MAX_BLOG_PRE_FILTER})")
    return result


# ── Tagging + scoring ───────────────────────────────────────────

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
    company = detect_company(article)
    if company:
        article["category"] = "company"
        article["company_tag"] = company
    elif detect_ai(article):
        article["category"] = "ai"
        article["company_tag"] = "AI技术"
    else:
        article["category"] = "other"
        article["company_tag"] = "市场动态"


def _keyword_score(article: dict) -> int:
    text = (article.get("title", "") + " " + article.get("summary", "")).lower()
    score = 1
    for w in HIGH_SIGNAL_WORDS:
        if w in text:
            score += 3
    for w in MID_SIGNAL_WORDS:
        if w in text:
            score += 1
    # v9: FOCUS_COMPANIES 额外加分，确保在 keyword fallback 场景下也能优先排序
    tag = article.get("company_tag")
    if tag in FOCUS_COMPANIES:
        score += 5
    elif tag not in [None, "市场动态"]:
        score += 3
    if article.get("source_type") == "media":
        score += 1
    if article.get("source_type") == "blog":
        score += 1  # give blogs a small floor so they are not all crowded out
    return min(10, max(1, score))


def call_gemini(prompt: str) -> str:
    if not GEMINI_API_KEY:
        raise EnvironmentError("请在 config.env 中设置 GEMINI_API_KEY")
    url = (
        "https://generativelanguage.googleapis.com/v1beta/"
        f"models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    )
    last_err = None
    for attempt in range(3):
        try:
            resp = requests.post(
                url,
                headers={"Content-Type": "application/json"},
                json={"contents": [{"parts": [{"text": prompt}]}]},
                timeout=120,
            )
            resp.raise_for_status()
            data = resp.json()
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()
        except Exception as e:
            last_err = e
            if attempt < 2:
                import time
                print(f"[WARN] Gemini attempt {attempt + 1} failed ({e}), retrying...")
                time.sleep(2)
    raise RuntimeError(f"Gemini 失败（3次重试后）: {last_err}")


def llm_tag_and_rank(articles: list[dict], limit: int | None = None) -> list[dict]:
    limit = limit or LLM_RANK_LIMIT
    companies = list(COMPANY_KEYWORDS.keys())

    lines = []
    for i, a in enumerate(articles):
        lines.append(
            f"{i}. 标题: {a['title']}\n"
            f"   摘要: {a['summary'][:160]}\n"
            f"   来源类型: {a.get('source_type', 'other')}\n"
            f"   来源: {a.get('source_display') or a.get('source')}"
        )

    prompt = f"""你是一名科技与互联网投研研究员。请对以下新闻分类并评分重要性。

分类规则：
- category="company"：涉及以下公司之一（填入company字段）：{", ".join(companies)}
- category="ai"：涉及AI/LLM/机器学习/大模型/生成式AI等，company填"AI技术"
- category="other"：其他新闻，company填"市场动态"

重要性评分（1-10）：
- 8-10：财报/监管/反垄断/收购/IPO/重大产品发布/CEO变动/重大商用落地
- 5-7：战略合作/市场扩张/新功能/行业报告/关键产品更新
- 1-4：普通资讯/观点文章/一般博客

额外要求：
- 不要因为是博客就全部打低分；若内容有明确方法论、技术拆解或行业洞见，可给 4-6 分
- 输出中必须覆盖全部新闻 id
- 只输出 JSON 数组，不要任何解释

格式：
[{{"id":0,"category":"company","company":"Amazon","importance":8}}]

新闻：
{chr(10).join(lines)}
"""

    try:
        text = call_gemini(prompt)
        match = re.search(r"\[.*\]", text, re.S)
        if not match:
            raise ValueError("no JSON array")
        tagged = json.loads(match.group(0))

        covered = set()
        for t in tagged:
            idx = t.get("id")
            if idx is None or not (0 <= idx < len(articles)):
                continue
            covered.add(idx)
            articles[idx]["category"] = t.get("category", "other")
            articles[idx]["company_tag"] = t.get("company", "市场动态")
            # v9: 在 LLM 重要性分基础上，给核心公司文章额外加 2 分（上限 10）
            base_score = min(10, max(1, int(t.get("importance", 5))))
            focus_bonus = 2 if articles[idx].get("company_tag") in FOCUS_COMPANIES else 0
            articles[idx]["score"] = min(10, base_score + focus_bonus)

        for idx, a in enumerate(articles):
            if idx not in covered:
                _keyword_tag(a)
                a["score"] = _keyword_score(a)

    except Exception as e:
        print(f"[WARN] LLM tagging failed ({e}), falling back to keyword tagging")
        for a in articles:
            _keyword_tag(a)
            a["score"] = _keyword_score(a)

    company_count = sum(1 for a in articles if a.get("category") == "company")
    ai_count = sum(1 for a in articles if a.get("category") == "ai")
    blog_count = sum(1 for a in articles if a.get("source_type") == "blog")
    focus_count = sum(1 for a in articles if a.get("company_tag") in FOCUS_COMPANIES)
    print(f"[{now_str()}] LLM tagged — Focus:{focus_count} | Company:{company_count} | AI:{ai_count} | Blog:{blog_count} | Total:{len(articles)}")

    articles.sort(
        key=lambda x: (
            x.get("score", 0),
            1 if x.get("source_type") == "media" else 0,
            x.get("published_dt") or datetime.datetime.min,
        ),
        reverse=True,
    )
    selected = articles[:limit]
    print(f"[{now_str()}] After LLM ranking: top {len(selected)} (limit={limit})")
    return selected


def reserve_diverse_articles(ranked: list[dict]) -> list[dict]:
    company = [a for a in ranked if a.get("category") == "company"]
    ai = [a for a in ranked if a.get("category") == "ai"]
    blogs = [a for a in ranked if a.get("source_type") == "blog"]
    others = [a for a in ranked if a not in company and a not in ai]

    selected: list[dict] = []
    seen = set()

    def pick(pool: list[dict], n: int):
        for a in pool:
            key = a.get("link") or a.get("title_hash")
            if key in seen:
                continue
            selected.append(a)
            seen.add(key)
            if len([x for x in selected if x in pool]) >= n:
                break

    pick(company, MIN_COMPANY_ITEMS)
    pick(ai, MIN_AI_ITEMS)
    pick(blogs, MIN_BLOG_ITEMS)

    for pool in (company, ai, blogs, others):
        for a in pool:
            key = a.get("link") or a.get("title_hash")
            if key in seen:
                continue
            selected.append(a)
            seen.add(key)

    print(
        f"[{now_str()}] Diversity reserve — Company>={MIN_COMPANY_ITEMS}, AI>={MIN_AI_ITEMS}, Blog>={MIN_BLOG_ITEMS}; selected={len(selected)}"
    )
    return selected


# ── v10: Full text fetch ─────────────────────────────────────────

def fetch_full_text(url: str) -> str:
    try:
        from newspaper import Article, Config
        cfg = Config()
        cfg.request_timeout = HTTP_TIMEOUT
        art = Article(url, config=cfg)
        art.download()
        art.parse()
        return art.text[:5000]
    except Exception:
        return ""


# ── Headline generation ─────────────────────────────────────────

def build_company_grouped_headlines(articles: list[dict], ai_mode: bool = False) -> list[dict]:
    lines = []
    for i, a in enumerate(articles, 1):
        # v10: 优先使用全文，回退到 summary
        content = a.get("full_text") or a.get("summary") or ""
        lines.append(
            f"{i}. [分类: {a.get('company_tag', '市场动态')}]\n"
            f"   标题: {a['title']}\n"
            f"   摘要: {content[:2000]}\n"
            f"   来源: {a.get('source_display') or a.get('source')}\n"
            f"   链接: {a['link']}"
        )

    if ai_mode:
        prompt = f"""你是一名AI技术分析师。请把下面的AI/技术新闻改写为精炼的中文投研 headline。

要求：
1. 每条新闻改写成一句话，包含：文章核心观点 + 关键技术概念 + 行业影响
2. 分类已在"[分类: ...]"字段中标注，按此分组输出，不要改变分组
3. 每条新闻保留 source 和 link
4. 语言简洁清晰，适合投资研究阅读
5. 所有 headline 必须用中文输出（即使原文是英文）
6. 只输出 JSON，不要任何解释

格式：
[
  {{
    "company": "AI技术",
    "items": [
      {{"headline": "Anthropic在非高峰时段临时上调Claude使用配额，反映其通过流量调度提升算力利用率", "source": "Engadget", "link": "https://example.com"}}
    ]
  }}
]

新闻：
{chr(10).join(lines)}
"""
    else:
        prompt = f"""你是一名中文投研新闻编辑。请把下面的新闻改写为精炼的中文投研 headline。

要求：
1. 每条新闻改写成一句话：事件 + 可能的影响
2. 分类已在"[分类: ...]"字段中标注，按此分组输出，不要改变分组
3. 每条新闻保留 source 和 link
4. 所有 headline 必须用中文输出（即使原文是英文）
5. 只输出 JSON，不要任何解释

格式：
[
  {{
    "company": "Amazon",
    "items": [
      {{"headline": "Amazon扩大Prime视频内容投入，或加大对流媒体留存与生态协同的布局", "source": "Reuters", "link": "https://example.com"}}
    ]
  }}
]

新闻：
{chr(10).join(lines)}
"""

    try:
        text = call_gemini(prompt)
        match = re.search(r"\[.*\]", text, re.S)
        if not match:
            raise ValueError("Gemini 未返回 JSON 数组")
        data = json.loads(match.group(0))
        # Prefer the original article's source_display over whatever Gemini returned.
        link_to_src = {
            a.get("link", ""): (a.get("source_display") or a.get("source", ""))
            for a in articles
        }
        for block in data:
            for item in block.get("items", []):
                orig = link_to_src.get(item.get("link", ""))
                if orig:
                    item["source"] = orig
                elif item.get("source", "").endswith(".com"):
                    item["source"] = prettify_domain(item["source"])
        return data
    except Exception as e:
        raise RuntimeError(f"Headline 生成失败: {e}")


def build_brief(articles: list[dict]) -> str:
    lines = [
        f"{i}. [公司: {a.get('company_tag', '未知')}] {a['title']}\n"
        f"   摘要: {a['summary']}\n"
        f"   来源: {a.get('source_display') or a.get('source')}"
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


# ── Markdown save ───────────────────────────────────────────────

def save_markdown_digest(grouped: list[dict], raw_articles: list[dict], label: str = "news-digest") -> str:
    today = now_local().strftime("%Y-%m-%d")
    ts = now_local().strftime("%Y-%m-%d_%H%M")
    path = os.path.join(OUTPUT_DIR, f"{ts}-{label}.md")
    title = "AI技术速递" if label == "ai-digest" else "新闻速递"

    lines = [
        "---", f"date: {today}", "tags:", "  - auto-generated", f"  - {label}",
        f"sources: {len(raw_articles)} articles", "---", "", f"# {title} {today}", "",
    ]
    for block in grouped:
        company = block.get("company", "未分类")
        items = block.get("items", [])
        if not items:
            continue
        lines.append(f"## {company}")
        for item in items:
            hl = item.get("headline", "").strip()
            source = item.get("source", "未知来源").strip()
            link = item.get("link", "").strip()
            lines.append(f"- {hl}（[{source}]({link})）" if link else f"- {hl}（{source}）")
        lines.append("")

    lines += ["---", "", "## Source Appendix", ""]
    for a in raw_articles:
        tag = a.get("company_tag", "")
        src = a.get("source_display") or a.get("source", "未知来源")
        lines.append(f"- [{a['title']}]({a['link']}) — {src}" + (f" `{tag}`" if tag else ""))

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[{now_str()}] Saved digest: {path}")
    return path


def save_markdown_brief(brief_text: str, raw_articles: list[dict]) -> str:
    today = now_local().strftime("%Y-%m-%d")
    ts = now_local().strftime("%Y-%m-%d_%H%M")
    path = os.path.join(OUTPUT_DIR, f"{ts}-investment-brief.md")

    lines = [
        "---", f"date: {today}", "tags:", "  - auto-generated", "  - investment-brief",
        f"sources: {len(raw_articles)} articles", "---", "", f"# 投研 Brief {today}", "", brief_text, "", "---", "", "## Source Appendix", "",
    ]
    for a in raw_articles:
        tag = a.get("company_tag", "")
        src = a.get("source_display") or a.get("source", "未知来源")
        lines.append(f"- [{a['title']}]({a['link']}) — {src}" + (f" `{tag}`" if tag else ""))

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[{now_str()}] Saved brief: {path}")
    return path


# ── Telegram ────────────────────────────────────────────────────

def escape_html(text: str) -> str:
    return html.escape(text or "", quote=True)


def build_telegram_message(grouped: list[dict], max_items: int = MAX_PUSH_ITEMS) -> str:
    lines = []
    total = 0
    for block in grouped:
        if total >= max_items:
            break
        company = escape_html(block.get("company", "未分类"))
        items = block.get("items", [])
        if not items:
            continue
        lines.append(f"<b>{company}</b>")
        for item in items:
            if total >= max_items:
                break
            hl = escape_html(item.get("headline", "").strip())
            source = escape_html(item.get("source", "未知来源").strip())
            link = item.get("link", "").strip()
            if link:
                lines.append(f'• {hl}（<a href="{html.escape(link, quote=True)}">{source}</a>）')
            else:
                lines.append(f"• {hl}（{source}）")
            total += 1
        lines.append("")

    text = "\n".join(lines).strip()
    return text[:TELEGRAM_MAX_CHARS].rstrip()


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
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=30,
    )
    resp.raise_for_status()
    print(f"[{now_str()}] Telegram sent.")


# v9: 4 条消息分桶推送
def send_four_messages(
    focus_grouped: list[dict],
    global_grouped: list[dict],
    ai_grouped: list[dict],
    blog_grouped: list[dict] | None = None,
):
    today = now_local().strftime("%Y-%m-%d")
    focus_text = build_telegram_message(focus_grouped)
    global_text = build_telegram_message(global_grouped)
    ai_text = build_telegram_message(ai_grouped)
    if focus_text:
        send_telegram(f"🎯 <b>核心公司</b> {today}\n\n" + focus_text)
    if global_text:
        send_telegram(f"🌍 <b>Global Tech</b> {today}\n\n" + global_text)
    if ai_text:
        send_telegram(f"🤖 <b>AI技术</b> {today}\n\n" + ai_text)
    if blog_grouped:
        blog_text = build_telegram_message(blog_grouped, max_items=MIN_BLOG_ITEMS)
        if blog_text:
            send_telegram(f"📖 <b>博客精选</b> {today}\n\n" + blog_text)


# v10: Bot1 原始 feed 推送（多条消息分批，按时间倒序）
def send_raw_feed(articles: list[dict]):
    token = TELEGRAM_RAW_BOT_TOKEN
    chat_id = TELEGRAM_RAW_CHAT_ID
    if not token or not chat_id:
        print("[BOT1 RAW] skipped (no TELEGRAM_RAW_BOT_TOKEN/CHAT_ID configured)")
        return

    sorted_arts = sorted(
        articles,
        key=lambda x: x.get("published_dt") or datetime.datetime.min,
        reverse=True,
    )[:RAW_FEED_LIMIT]

    today = now_local().strftime("%Y-%m-%d")
    header = f"📰 <b>最新资讯</b> {today}（{len(sorted_arts)} 条）\n\n"

    lines = []
    for a in sorted_arts:
        title = escape_html(a["title"])
        source = escape_html(a.get("source_display") or a.get("source", ""))
        link = a.get("link", "")
        if link:
            lines.append(f'• {title}（<a href="{html.escape(link, quote=True)}">{source}</a>）')
        else:
            lines.append(f"• {title}（{source}）")

    # 分批发送，每批不超过 TELEGRAM_MAX_CHARS
    current = header
    sent_count = 0
    for line in lines:
        addition = line + "\n"
        if len(current) + len(addition) > TELEGRAM_MAX_CHARS and current.strip():
            try:
                requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={
                        "chat_id": chat_id,
                        "text": current.strip(),
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    },
                    timeout=15,
                ).raise_for_status()
                sent_count += 1
            except Exception as e:
                print(f"[BOT1 RAW] Send error: {e}")
            current = ""
        current += addition

    if current.strip():
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": current.strip(),
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            ).raise_for_status()
            sent_count += 1
        except Exception as e:
            print(f"[BOT1 RAW] Send error: {e}")

    print(f"[BOT1 RAW] Sent {len(sorted_arts)} articles in {sent_count} message(s)")


# ── Main ────────────────────────────────────────────────────────

def main():
    ensure_dirs()
    import logging
    logging.basicConfig(
        filename=os.path.join("logs", "digest.log"),
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logging.info("=== digest v10 start ===")
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

    # Bot1: 发送原始 feed（全量新文章，未经 fast_filter）
    if SEND_TELEGRAM:
        send_raw_feed(articles_new)

    # v10: fast_filter 防止博客挤占 LLM 配额
    articles_filtered = fast_filter(articles_new)

    ranked = llm_tag_and_rank(articles_filtered)

    if MIN_ARTICLE_SCORE > 0:
        ranked = [a for a in ranked if a.get("score", 0) >= MIN_ARTICLE_SCORE]
        print(f"[{now_str()}] After score filter (>={MIN_ARTICLE_SCORE}): {len(ranked)}")

    if not ranked:
        print(f"[{now_str()}] No articles passed quality gate, skipping.")
        return

    # v10: 对 top N 篇文章抓取全文，供 Gemini headline 使用
    print(f"[{now_str()}] Fetching full text for top {TOP_FULLTEXT} articles...")
    for a in ranked[:TOP_FULLTEXT]:
        a["full_text"] = fetch_full_text(a["link"])
    fulltext_ok = sum(1 for a in ranked[:TOP_FULLTEXT] if a.get("full_text"))
    print(f"[{now_str()}] Full text fetched: {fulltext_ok}/{min(TOP_FULLTEXT, len(ranked))} articles")

    ranked = reserve_diverse_articles(ranked)

    # v9: 4 桶分发，每篇文章只进入第一个匹配的桶
    focus_articles, global_articles, ai_articles, blog_articles = [], [], [], []
    seen_keys: set[str] = set()

    for a in ranked:
        key = a.get("link") or a.get("title_hash")
        if key in seen_keys:
            continue
        tag = a.get("company_tag", "")
        cat = a.get("category", "other")
        src = a.get("source_type", "other")

        if tag in FOCUS_COMPANIES:
            focus_articles.append(a)
        elif tag in GLOBAL_TECH:
            global_articles.append(a)
        elif cat == "ai":
            ai_articles.append(a)
        elif src == "blog":
            blog_articles.append(a)
        else:
            continue  # "other" 类不推送，也不记入 sent

        seen_keys.add(key)

    focus_articles  = focus_articles[:MAX_PUSH_ITEMS]
    global_articles = global_articles[:MAX_PUSH_ITEMS]
    ai_articles     = ai_articles[:MAX_PUSH_ITEMS]
    blog_articles   = blog_articles[:MIN_BLOG_ITEMS]

    print(
        f"[{now_str()}] Buckets — Focus:{len(focus_articles)} | Global:{len(global_articles)}"
        f" | AI:{len(ai_articles)} | Blog:{len(blog_articles)}"
    )

    digest_files = []
    focus_grouped, global_grouped, ai_grouped, blog_grouped = [], [], [], []

    if focus_articles:
        focus_grouped = build_company_grouped_headlines(focus_articles)
        digest_files.append(save_markdown_digest(focus_grouped, focus_articles, label="focus-digest"))

    if global_articles:
        global_grouped = build_company_grouped_headlines(global_articles)
        digest_files.append(save_markdown_digest(global_grouped, global_articles, label="global-digest"))

    if ai_articles:
        ai_grouped = build_company_grouped_headlines(ai_articles, ai_mode=True)
        digest_files.append(save_markdown_digest(ai_grouped, ai_articles, label="ai-digest"))

    if blog_articles:
        blog_grouped = build_company_grouped_headlines(blog_articles)
        digest_files.append(save_markdown_digest(blog_grouped, blog_articles, label="blog-digest"))

    brief_file = None
    if focus_articles:
        try:
            brief_text = build_brief(focus_articles)
            brief_file = save_markdown_brief(brief_text, focus_articles)
        except Exception as e:
            print(f"[WARN] Brief generation failed ({e}), skipping brief.")

    # 只记录实际推送的文章，"other" 类文章不消耗 sent 配额
    actually_sent = focus_articles + global_articles + ai_articles + blog_articles
    sent_store = update_sent_store(sent_store, actually_sent)
    save_sent(sent_store)

    send_four_messages(focus_grouped, global_grouped, ai_grouped, blog_grouped or None)

    summary = " | ".join(digest_files) if digest_files else "(no digest)"
    if brief_file:
        summary += f" | Brief: {brief_file}"
    print(f"[{now_str()}] Done. {summary}")


if __name__ == "__main__":
    main()
