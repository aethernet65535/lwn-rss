#!/usr/bin/env python3
"""
LWN.net Kernel free-article RSS aggregator.

Periodically fetches the full LWN Kernel Index
(https://lwn.net/Kernel/Index/), keeps only articles that have passed
LWN's free-access waiting period (14 days by default), and publishes a
standard RSS 2.0 feed over HTTP.
"""

from __future__ import annotations

import argparse
import datetime as dt
import email.utils
import html.parser
import http.server
import os
import re
import socket
import socketserver
import sys
import threading
import time
import urllib.request
import xml.sax.saxutils

DEFAULT_LWN_URL = "https://lwn.net/Kernel/Index/"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000
DEFAULT_REFRESH_SECONDS = 24 * 60 * 60
DEFAULT_RETRY_SECONDS = 60 * 60
DEFAULT_FREE_AFTER_DAYS = 14
DEFAULT_PERIOD = "month"
PERIOD_DAYS = {
    "week": 7,
    "month": 30,
    "year": 365,
    "all": None,
}
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 "
    "LWN-FreeRSS/1.0"
)
DEFAULT_PUBLIC_BASE_URL = ""


class LWNKernelParser(html.parser.HTMLParser):
    """Parse LWN Kernel article listings.

    Supports both the recent-articles table on /Kernel/ and the topic index
    entries on /Kernel/Index/.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.in_tr = False
        self.current_td = None
        self.row: list[dict[str, str]] = []
        self.in_index_entry = False
        self.index_buf: list[str] = []
        self.index_href = ""
        self.articles: list[dict[str, object]] = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "tr":
            self.in_tr = True
            self.row = []
        elif tag == "td" and self.in_tr:
            self.current_td = {"text": [], "href": ""}
        elif tag == "a" and self.current_td is not None:
            self.current_td["href"] = attrs.get("href", "")
        elif tag == "p" and "IndexEntry" in attrs.get("class", "").split():
            self.in_index_entry = True
            self.index_buf = []
            self.index_href = ""
        elif tag == "a" and self.in_index_entry:
            href = attrs.get("href", "")
            if href.startswith("/Articles/"):
                self.index_href = href

    def handle_data(self, data):
        if self.current_td is not None:
            self.current_td["text"].append(data)
        if self.in_index_entry:
            self.index_buf.append(data)

    def handle_endtag(self, tag):
        if tag == "td" and self.current_td is not None:
            text = " ".join("".join(self.current_td["text"]).split())
            self.row.append({"text": text, "href": self.current_td["href"]})
            self.current_td = None
        elif tag == "tr" and self.in_tr:
            self.in_tr = False
            if len(self.row) >= 2:
                date_text = self.row[0]["text"]
                title = self.row[1]["text"]
                href = self.row[1]["href"]
                if date_text and title and href.startswith("/Articles/"):
                    try:
                        pub_date = parse_lwn_date(date_text)
                    except ValueError:
                        pub_date = None
                    if pub_date is not None:
                        self.articles.append(
                            {
                                "title": title,
                                "url": href,
                                "date": pub_date,
                            }
                        )
            self.row = []
        elif tag == "p" and self.in_index_entry:
            self.in_index_entry = False
            if self.index_href:
                text = " ".join("".join(self.index_buf).split())
                # Example: "Realtime Linux: academia v. reality (July 26, 2010)"
                match = re.match(
                    r"^(.*?)\s*\(([A-Z][a-z]+ \d{1,2}, \d{4})\)$", text
                )
                if match:
                    title = match.group(1).strip()
                    try:
                        pub_date = parse_lwn_date(match.group(2))
                    except ValueError:
                        pub_date = None
                    if title and pub_date is not None:
                        self.articles.append(
                            {
                                "title": title,
                                "url": self.index_href,
                                "date": pub_date,
                            }
                        )
            self.index_buf = []
            self.index_href = ""


def parse_lwn_date(text: str) -> dt.date:
    """Parse LWN date strings like 'August 28, 2026'."""
    return dt.datetime.strptime(text.strip(), "%B %d, %Y").date()


def parse_oldest_date(text: str) -> dt.date:
    """Parse a user-supplied oldest date in YYYY-MM-DD format."""
    return dt.datetime.strptime(text.strip(), "%Y-%m-%d").date()


def period_oldest_date(
    period: str, today: dt.date | None = None
) -> dt.date | None:
    """Return the oldest allowed date for a quick period option.

    ``all`` means no lower bound.  ``month`` is the last 30 days and
    ``year`` is the last 365 days.
    """
    today = today or dt.date.today()
    if period not in PERIOD_DAYS:
        raise ValueError(f"Unknown period: {period}")
    days = PERIOD_DAYS[period]
    if days is None:
        return None
    return today - dt.timedelta(days=days)


def detect_lan_ip() -> str:
    """Detect the host's primary LAN IPv4 address."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # UDP connect does not send packets; it only selects the outgoing route.
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
    except OSError:
        ip = "127.0.0.1"
    finally:
        sock.close()
    return ip


def auto_public_base_url(port: int) -> str:
    """Build a LAN URL like http://192.168.1.10:8000 using the detected IP."""
    ip = detect_lan_ip()
    if port == 80:
        return f"http://{ip}"
    return f"http://{ip}:{port}"


def fetch_kernel_articles(
    url: str = DEFAULT_LWN_URL, user_agent: str = DEFAULT_USER_AGENT
) -> list[dict]:
    """Download the LWN Kernel index and return raw article metadata."""
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    parser = LWNKernelParser()
    parser.feed(raw)
    return parser.articles


def absolute_url(href: str, base: str = "https://lwn.net") -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return base + href
    return base + "/" + href


def filter_free_articles(
    articles: list[dict],
    today: dt.date | None = None,
    free_after_days: int = DEFAULT_FREE_AFTER_DAYS,
    oldest_date: dt.date | None = None,
) -> list[dict]:
    """Keep free articles within the requested date window.

    Articles must be at least ``free_after_days`` old, and must not be older
    than ``oldest_date`` (when set).  Older saved entries are therefore removed
    on the next refresh.
    """
    today = today or dt.date.today()
    result = []
    seen = set()
    for article in articles:
        pub_date = article.get("date")
        if not isinstance(pub_date, dt.date):
            continue
        if (today - pub_date).days < free_after_days:
            continue
        if oldest_date is not None and pub_date < oldest_date:
            continue
        url = absolute_url(str(article["url"]))
        if url in seen:
            continue
        seen.add(url)
        result.append(
            {
                "title": str(article["title"]),
                "url": url,
                "date": pub_date,
            }
        )
    # Newest first
    result.sort(key=lambda item: item["date"], reverse=True)
    return result


def rfc2822(date: dt.date) -> str:
    """Return an RFC 2822 date string for an LWN publication date."""
    return email.utils.format_datetime(
        dt.datetime(date.year, date.month, date.day, tzinfo=dt.timezone.utc)
    )


def build_rss(
    articles: list[dict],
    public_base_url: str = DEFAULT_PUBLIC_BASE_URL,
    feed_title: str = "LWN Kernel Free Articles",
) -> str:
    """Generate an RSS 2.0 XML document."""
    if not public_base_url:
        public_base_url = "http://localhost:8000"
    public_base_url = public_base_url.rstrip("/")
    feed_url = public_base_url + "/rss.xml"
    now = dt.datetime.now(dt.timezone.utc)
    lines = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append(
        '<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">'
    )
    lines.append("  <channel>")
    lines.append(f"    <title>{xml_escape(feed_title)}</title>")
    lines.append(f"    <link>{xml_escape(public_base_url)}</link>")
    lines.append(
        "    <description>"
        "Free LWN.net Kernel articles, published after the 14-day paywall grace period."
        "</description>"
    )
    lines.append("    <language>en-us</language>")
    lines.append(
        f"    <lastBuildDate>{email.utils.format_datetime(now)}</lastBuildDate>"
    )
    lines.append(
        f'    <atom:link href="{xml_escape(feed_url)}" '
        'rel="self" type="application/rss+xml"/>'
    )
    lines.append("    <ttl>1440</ttl>")
    for article in articles:
        title = xml_escape(article["title"])
        url = xml_escape(article["url"])
        pub_date = rfc2822(article["date"])
        lines.append("    <item>")
        lines.append(f"      <title>{title}</title>")
        lines.append(f"      <link>{url}</link>")
        lines.append(f'      <guid isPermaLink="true">{url}</guid>')
        lines.append(f"      <pubDate>{pub_date}</pubDate>")
        lines.append(f"      <description>{title}</description>")
        lines.append("    </item>")
    lines.append("  </channel>")
    lines.append("</rss>")
    return "\n".join(lines) + "\n"


def xml_escape(text: str) -> str:
    return xml.sax.saxutils.escape(text, {'"': "&quot;"})


def refresh(
    rss_path: str,
    lwn_url: str = DEFAULT_LWN_URL,
    free_after_days: int = DEFAULT_FREE_AFTER_DAYS,
    public_base_url: str = DEFAULT_PUBLIC_BASE_URL,
    user_agent: str = DEFAULT_USER_AGENT,
    oldest_date: dt.date | None = None,
) -> bool:
    """Fetch, filter, and atomically write the RSS file.

    Returns True on success. On any error the previous RSS file is left
    intact.
    """
    try:
        raw_articles = fetch_kernel_articles(lwn_url, user_agent)
        if not raw_articles:
            raise RuntimeError("No articles parsed from LWN page")
        articles = filter_free_articles(
            raw_articles,
            free_after_days=free_after_days,
            oldest_date=oldest_date,
        )
        rss = build_rss(articles, public_base_url=public_base_url)
        directory = os.path.dirname(os.path.abspath(rss_path))
        os.makedirs(directory, exist_ok=True)
        tmp_path = rss_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(rss)
        os.replace(tmp_path, rss_path)
        print(
            f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] "
            f"Refreshed RSS: {len(articles)} free articles -> {rss_path}",
            flush=True,
        )
        return True
    except Exception as exc:  # noqa: BLE001 - intentionally keep service alive
        print(
            f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] "
            f"Refresh failed, keeping previous RSS: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return False


class RSSHandler(http.server.BaseHTTPRequestHandler):
    """Serve the generated RSS file."""

    rss_path = "lwn_kernel_free.xml"

    def do_GET(self):  # noqa: N802
        if self.path.split("?")[0] in ("/", "/rss.xml", "/feed", "/index.xml"):
            self.serve_rss()
        else:
            self.send_error(404, "Not Found")

    def serve_rss(self):
        try:
            with open(self.rss_path, "rb") as f:
                body = f.read()
        except FileNotFoundError:
            self.send_error(503, "RSS has not been generated yet")
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/rss+xml; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        sys.stderr.write(
            "[http] %s - %s\n" % (self.address_string(), fmt % args)
        )


class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def refresh_loop(
    rss_path: str,
    interval: int,
    retry_interval: int = DEFAULT_RETRY_SECONDS,
    last_ok: bool = True,
    **kwargs,
) -> None:
    while True:
        time.sleep(interval if last_ok else retry_interval)
        last_ok = refresh(rss_path, **kwargs)


def env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--version", action="version", version="%(prog)s 1.0.0"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Fetch and write RSS once, then exit.",
    )
    parser.add_argument("--host", default=os.environ.get("HOST", DEFAULT_HOST))
    parser.add_argument(
        "--port", type=int, default=env_int("PORT", DEFAULT_PORT)
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=env_int("REFRESH_INTERVAL_SECONDS", DEFAULT_REFRESH_SECONDS),
    )
    parser.add_argument(
        "--retry-interval",
        type=int,
        default=env_int("RETRY_INTERVAL_SECONDS", DEFAULT_RETRY_SECONDS),
    )
    parser.add_argument(
        "--free-after-days",
        type=int,
        default=env_int("FREE_AFTER_DAYS", DEFAULT_FREE_AFTER_DAYS),
    )
    parser.add_argument(
        "--period",
        choices=tuple(PERIOD_DAYS.keys()),
        default=os.environ.get("PERIOD", DEFAULT_PERIOD),
        help="Quick date range: week, month, year, or all (default: month)",
    )
    parser.add_argument(
        "--oldest-date",
        default=os.environ.get("OLDEST_DATE", ""),
        help=(
            "Custom oldest article date to keep, "
            "format YYYY-MM-DD (overrides --period)"
        ),
    )
    parser.add_argument(
        "--lwn-url", default=os.environ.get("LWN_URL", DEFAULT_LWN_URL)
    )
    parser.add_argument(
        "--public-base-url",
        default=os.environ.get("PUBLIC_BASE_URL", DEFAULT_PUBLIC_BASE_URL),
    )
    parser.add_argument(
        "--user-agent",
        default=os.environ.get("USER_AGENT", DEFAULT_USER_AGENT),
    )
    parser.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "."))
    args = parser.parse_args(argv)

    rss_path = os.path.abspath(
        os.path.join(args.data_dir, "lwn_kernel_free.xml")
    )
    public_base_url = (
        args.public_base_url.strip() if args.public_base_url else ""
    )
    if not public_base_url:
        public_base_url = auto_public_base_url(args.port)

    oldest_date = None
    if args.oldest_date:
        try:
            oldest_date = parse_oldest_date(args.oldest_date)
        except ValueError:
            parser.error("--oldest-date must be in YYYY-MM-DD format")
    else:
        oldest_date = period_oldest_date(args.period)

    if args.once:
        ok = refresh(
            rss_path,
            lwn_url=args.lwn_url,
            free_after_days=args.free_after_days,
            oldest_date=oldest_date,
            public_base_url=public_base_url,
            user_agent=args.user_agent,
        )
        return 0 if ok else 1

    # Initial refresh before serving. If it fails, the server still starts and
    # will serve the last good file (or 503 if none exists yet).
    last_ok = refresh(
        rss_path,
        lwn_url=args.lwn_url,
        free_after_days=args.free_after_days,
        oldest_date=oldest_date,
        public_base_url=public_base_url,
        user_agent=args.user_agent,
    )

    RSSHandler.rss_path = rss_path
    server = ThreadingHTTPServer((args.host, args.port), RSSHandler)
    thread = threading.Thread(
        target=refresh_loop,
        args=(rss_path, args.interval, args.retry_interval, last_ok),
        kwargs={
            "lwn_url": args.lwn_url,
            "free_after_days": args.free_after_days,
            "oldest_date": oldest_date,
            "public_base_url": public_base_url,
            "user_agent": args.user_agent,
        },
        daemon=True,
    )
    thread.start()
    print(
        f"Serving RSS at http://{args.host}:{args.port}/rss.xml\n\n"
        f"Feed URL:\n\t{public_base_url}/rss.xml",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
