# .github/scripts/fetch_news.py
from __future__ import annotations

import csv
import os
import time
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import quote_plus
import xml.etree.ElementTree as ET

import requests

# =========================
# Config
# =========================
JST = timezone(timedelta(hours=9))

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ここを編集したくない場合は、config/queries.txt を置く（1行1クエリ）
QUERY_FILE = Path("config/queries.txt")

# config/queries.txt が無い場合のフォールバック
DEFAULT_QUERIES = [
    "物価 食料 日本",
    "米 小麦 価格",
    "コンビニ 新商品",
]

# 1クエリあたりの最大取得件数（RSSは上限があるので、取りすぎない）
MAX_ITEMS_PER_QUERY = int(os.getenv("MAX_ITEMS_PER_QUERY", "60"))

# 過負荷・BAN対策：クエリ間のスリープ（秒）
SLEEP_BETWEEN_QUERIES = float(os.getenv("SLEEP_BETWEEN_QUERIES", "1.5"))

# リクエストのタイムアウト
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))

# User-Agent（Actionsなどで弾かれにくくする）
UA = os.getenv("USER_AGENT") or "trend-bot/1.0 (+https://github.com/your-org/your-repo)"


# =========================
# Data structure
# =========================
@dataclass
class NewsItem:
    query: str
    title: str
    link: str
    published: str  # RFC822 string or empty
    collected_jst: str  # ISO string JST

    def as_row(self) -> Dict[str, str]:
        return {
            "source": f"GoogleNews:{self.query}",
            "query": self.query,
            "title": self.title,
            "link": self.link,
            "published": self.published,
            "collected_jst": self.collected_jst,
        }


# =========================
# Helpers
# =========================
def jst_today() -> date:
    return datetime.now(JST).date()


def iso_day(d: date) -> str:
    return d.isoformat()


def now_jst_iso() -> str:
    return datetime.now(JST).isoformat(timespec="seconds")


def google_news_rss_url(query: str, lang: str = "ja", country: str = "JP") -> str:
    # Google News RSS: https://news.google.com/rss/search?q=...&hl=...&gl=...&ceid=...
    q = quote_plus(query)
    ceid = f"{country}:{lang}"
    return f"https://news.google.com/rss/search?q={q}&hl={lang}&gl={country}&ceid={ceid}"


def parse_rss(xml_text: str, query: str, max_items: int) -> List[NewsItem]:
    items: List[NewsItem] = []
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return items

    channel = root.find("channel")
    if channel is None:
        return items

    for it in channel.findall("item")[:max_items]:
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        pub = (it.findtext("pubDate") or "").strip()

        if not title:
            continue

        items.append(
            NewsItem(
                query=query,
                title=title,
                link=link,
                published=pub,
                collected_jst=now_jst_iso(),
            )
        )
    return items


def load_queries_from_file(path: Path) -> List[str]:
    """
    config/queries.txt を読み込む（1行1クエリ）
    - 空行、# コメント行は無視
    """
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    out: List[str] = []
    for l in lines:
        s = (l or "").strip()
        if not s:
            continue
        if s.startswith("#"):
            continue
        out.append(s)
    return out


def read_existing_set(csv_path: Path) -> set:
    """
    既に当日CSVがある場合、title+link で重複排除できるように既存キーを読む
    """
    seen = set()
    if not csv_path.exists():
        return seen
    try:
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                t = (r.get("title") or "").strip()
                l = (r.get("link") or "").strip()
                if t and l:
                    seen.add((t, l))
    except Exception:
        pass
    return seen


def append_rows(csv_path: Path, rows: List[Dict[str, str]]) -> None:
    header = ["source", "query", "title", "link", "published", "collected_jst"]
    file_exists = csv_path.exists()

    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with csv_path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not file_exists:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})


def collect_daily(day: Optional[date] = None) -> Path:
    day = day or jst_today()
    out_csv = DATA_DIR / f"news_{iso_day(day)}.csv"

    # クエリ読み込み（queries.txt があればそれ優先）
    queries = load_queries_from_file(QUERY_FILE)
    if not queries:
        queries = DEFAULT_QUERIES

    seen = read_existing_set(out_csv)
    all_new_rows: List[Dict[str, str]] = []

    print(f"[daily] {iso_day(day)} queries={len(queries)} -> {out_csv}")
    if QUERY_FILE.exists():
        print(f"[daily] query_source=file:{QUERY_FILE}")
    else:
        print("[daily] query_source=DEFAULT_QUERIES (config/queries.txt not found)")

    s = requests.Session()
    s.headers.update({"User-Agent": UA})

    for i, q in enumerate(queries, start=1):
        url = google_news_rss_url(q)
        try:
            r = s.get(url, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            items = parse_rss(r.text, query=q, max_items=MAX_ITEMS_PER_QUERY)
        except Exception as e:
            print(f"[warn] query='{q}' fetch_failed={type(e).__name__}")
            items = []

        # 重複排除（title+link）
        new_items = []
        for it in items:
            key = (it.title, it.link)
            if key in seen:
                continue
            seen.add(key)
            new_items.append(it)

        all_new_rows.extend([it.as_row() for it in new_items])
        print(f"[ok] {i}/{len(queries)} query='{q}' got={len(items)} new={len(new_items)}")

        # クエリ間に軽く待つ（BAN/過負荷回避）
        time.sleep(SLEEP_BETWEEN_QUERIES + random.uniform(0, 0.5))

    if all_new_rows:
        append_rows(out_csv, all_new_rows)

    print(f"[done] appended={len(all_new_rows)}")
    return out_csv


if __name__ == "__main__":
    collect_daily()
