"""
ablibrary_scraper.py
====================
Scrapes books from v4.ablibrary.net into organized Markdown files.

How it works (no DOM scraping, no sidebars):
  1. Open the book page in a headless Playwright browser.
  2. The app automatically calls its gRPC backend (grpc.ablibrary.net) to fetch
     all pages, then caches them in IndexedDB ("BooksDatabase").
  3. We read the cached pages back out of IndexedDB via page.evaluate() —
     this gives us clean structured data, not scraped HTML.
  4. Recursively walk the proto-es content tree to extract text, headings,
     and footnotes, and write a Markdown file per book.

Usage:
  python ablibrary_scraper.py 5176 "Mu'jam Rijal al-Hadith Vol 1"
  python ablibrary_scraper.py          # batch mode, edit BOOKS_TO_SCRAPE below
"""

import json
import re
import sys
import time
from pathlib import Path

OUTPUT_DIR = Path("books_md")
OUTPUT_DIR.mkdir(exist_ok=True)

ABLIBRARY_BOOK_URL = "https://v4.ablibrary.net/books/{id}?tab-id=4"

# ──────────────────────────────────────────────────────────────────────────────
# Content-tree walker
# ──────────────────────────────────────────────────────────────────────────────

def _collect_text(node: dict) -> str:
    """Return all text inside a single PageContent node (leaf or branch)."""
    parts = []
    data = node.get("data") or {}
    case = data.get("case", "")
    value = data.get("value") or {}

    # Leaf: actual text string
    if case == "text":
        t = value.get("text", "").strip()
        if t:
            parts.append(t)

    # Recurse into children
    for child in node.get("children") or []:
        t = _collect_text(child)
        if t:
            parts.append(t)

    return " ".join(parts)


def page_to_md(page: dict) -> str:
    """Convert one IPage dict (from IndexedDB) into a Markdown section."""
    label = str(page.get("label") or page.get("number") or "?").strip()
    lines = [f"---\n\n## Page {label}\n"]

    for content_node in page.get("contents") or []:
        data = content_node.get("data") or {}
        case = data.get("case", "")
        value = data.get("value") or {}

        text = _collect_text(content_node).strip()
        if not text:
            continue

        if case == "heading":
            level = value.get("level", 2)
            hashes = "#" * (level + 2)   # h1→###, h2→####, etc.
            lines.append(f"\n{hashes} {text}\n")

        elif case == "footnote":
            lines.append(f"\n> **Footnote:** {text}\n")

        elif case == "poem":
            # Indent poem lines
            poem_lines = text.split()
            lines.append("\n" + "\n".join(f"    {l}" for l in poem_lines) + "\n")

        else:
            # paragraph, text, remark, ref, highlight, horizontal_line …
            lines.append(f"\n{text}\n")

    return "\n".join(lines)


def pages_to_markdown(book_id: str, pages: list[dict], title: str = "") -> str:
    header = title or f"Book {book_id}"
    out = [
        f"# {header}",
        f"> Source: Ahlulbayt Library (v4.ablibrary.net) · Book ID: {book_id}",
        "",
    ]
    for page in pages:
        out.append(page_to_md(page))
    return "\n".join(out)


# ──────────────────────────────────────────────────────────────────────────────
# IndexedDB extraction (runs inside the browser via page.evaluate)
# ──────────────────────────────────────────────────────────────────────────────

# JS that reads all pages for a given bookId from BooksDatabase
_IDBJS = """
async (bookId) => {
    return new Promise((resolve, reject) => {
        const req = indexedDB.open('BooksDatabase');
        req.onerror = () => reject('IDB open failed: ' + req.error);
        req.onsuccess = () => {
            const db = req.result;
            if (!db.objectStoreNames.contains('books')) {
                return resolve(null);
            }
            const tx = db.transaction('books', 'readonly');
            const store = tx.objectStore('books');

            // Try index first, fall back to full scan
            let getReq;
            if (store.indexNames.contains('bookId')) {
                getReq = store.index('bookId').get(bookId);
            } else {
                getReq = store.getAll();
            }

            getReq.onsuccess = () => {
                const result = getReq.result;
                if (!result) return resolve(null);
                // getAll returns array; index.get returns single record
                const record = Array.isArray(result)
                    ? result.find(r => String(r.bookId) === String(bookId))
                    : result;
                if (!record) return resolve(null);
                // Serialize safely (proto-es may use BigInt)
                try {
                    resolve(JSON.parse(JSON.stringify(record,
                        (_, v) => typeof v === 'bigint' ? Number(v) : v
                    )));
                } catch(e) {
                    reject('serialize error: ' + e);
                }
            };
            getReq.onerror = () => reject('IDB get failed: ' + getReq.error);
        };
    });
}
"""


# ──────────────────────────────────────────────────────────────────────────────
# Main scrape function
# ──────────────────────────────────────────────────────────────────────────────

def scrape_book(book_id: int | str, title: str = "") -> Path | None:
    """
    Scrape a single book by its ablibrary numeric ID and write a Markdown file.

    Args:
        book_id:  The numeric ID from the ablibrary URL (e.g. 5176).
        title:    Optional human-readable title for the Markdown header.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("Playwright not installed. Run:\n  pip install playwright\n  playwright install chromium")
        return None

    book_id = str(book_id)
    print(f"\n{'='*60}")
    print(f"Book {book_id}  |  {title or '(no title given)'}")

    url = ABLIBRARY_BOOK_URL.format(id=book_id)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print(f"  Opening {url} ...")
        page.goto(url, wait_until="networkidle", timeout=90_000)

        # Wait a bit extra: the gRPC call + IndexedDB write happens async
        # after the page renders. We wait up to ~30s for the DB to be populated.
        record = None
        for attempt in range(1, 7):
            time.sleep(5)
            print(f"  Checking IndexedDB (attempt {attempt}/6) ...")
            try:
                record = page.evaluate(_IDBJS, book_id)
            except Exception as e:
                print(f"    JS error: {e}")
                continue
            if record:
                break
            print("    Not in DB yet — waiting ...")

        browser.close()

    if not record:
        print(f"  ✗ Book {book_id} not found in IndexedDB after page load.")
        print("    Possible reasons:")
        print("    • The book ID doesn't exist on ablibrary")
        print("    • The book is OCR/PDF type (no text content)")
        print("    • The gRPC call took longer than expected")
        return None

    pages = record.get("content") or []
    book_type = record.get("type", "unknown")
    detail = record.get("detail") or {}
    auto_title = title or (
        detail.get("title") or
        detail.get("translations", [{}])[0].get("title", "") or
        f"Book {book_id}"
    )

    print(f"  ✓ Found in IndexedDB: {len(pages)} pages  (type={book_type})")

    if book_type == "ocr":
        print("  ⚠ This is an OCR/PDF book — no structured text content.")
        print("    Only page image metadata is available, not Arabic text.")
        return None

    md_content = pages_to_markdown(book_id, pages, auto_title)
    out_path = OUTPUT_DIR / f"{book_id}.md"
    out_path.write_text(md_content, encoding="utf-8")
    print(f"  ✓ Saved → {out_path}  ({len(pages)} pages, {len(md_content):,} chars)")
    return out_path


# ──────────────────────────────────────────────────────────────────────────────
# Batch list — edit this to scrape multiple books
# ──────────────────────────────────────────────────────────────────────────────

BOOKS_TO_SCRAPE = [
    # (book_id_from_url,  human_title)
    (7194, "Al Mufid min Rijal Al Hadith")
    # Add more books here
]


def main():
    if len(sys.argv) > 1:
        # CLI: python ablibrary_scraper.py 5176 "My Book Title"
        bid = sys.argv[1]
        ttl = " ".join(sys.argv[2:]) if len(sys.argv) > 2 else ""
        scrape_book(bid, ttl)
        return

    print(f"Batch scraping {len(BOOKS_TO_SCRAPE)} book(s) → {OUTPUT_DIR}/\n")
    for bid, ttl in BOOKS_TO_SCRAPE:
        scrape_book(bid, ttl)
        time.sleep(2)

    print(f"\nDone. Markdown files are in: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
