import asyncio
import json
from typing import Any, Dict, List, Optional
from playwright.async_api import async_playwright
from langchain_core.tools import StructuredTool


# In-memory browser sessions and simple key→value memory store
_browsers: Dict[str, Any] = {}
_memory: Dict[str, str] = {}

def _run(coro: asyncio.coroutines) -> Any:
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ---- Asynchronous implementations ----

async def _async_open_page(session_id: str, url: str) -> str:
    if session_id not in _browsers:
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"
        })
        _browsers[session_id] = (pw, browser, page)

    pw, browser, page = _browsers[session_id]
    print(f"[open_page] Navigating to: {url}")
    try:
        await page.goto(url, timeout=30000)
        content = await page.content()
        print(f"[open_page] Page content length: {len(content)}")
        return content
    except Exception as e:
        print(f"[open_page] Failed to load {url}: {e}")
        return "<html><body><h1>Error loading page</h1></body></html>"

async def _async_fill(session_id: str, selector: str, text: str) -> None:
    _, _, page = _browsers[session_id]
    await page.fill(selector, text)

async def _async_click(session_id: str, selector: str) -> None:
    _, _, page = _browsers[session_id]
    await page.click(selector, timeout=1000)

async def _async_extract(session_id: str, selector: str) -> List[str]:
    _, _, page = _browsers[session_id]
    elements = await page.query_selector_all(selector,timeout=600)
    return [await el.inner_text() for el in elements]


# ---- Synchronous wrappers ----

def open_page(session_id: str, url: str) -> str:
    return _run(_async_open_page(session_id, url))

def fill(session_id: str, selector: str, text: str) -> None:
    return _run(_async_fill(session_id, selector, text))

def click(session_id: str, selector: str) -> None:
    return _run(_async_click(session_id, selector))

def extract(session_id: str, selector: str) -> List[str]:
    result = _run(_async_extract(session_id, selector))
    print(f"[extract] result for '{selector}':", result)
    return result  # <== must return a valid list

def remember(key: str, value: Any) -> None:
    if isinstance(value, str):
        _memory[key] = value
    else:
        _memory[key] = json.dumps(value, ensure_ascii=False)

def recall(key: str) -> Optional[str]:
    return _memory.get(key)


# ---- StructuredTool definitions ----

open_page_tool = StructuredTool.from_function(
    func=open_page,
    name="open_page",
    description="(session_id, url) → Extract HTML DOM content from a webpage."
)

fill_tool = StructuredTool.from_function(
    func=fill,
    name="fill",
    description="(session_id, selector, text) → Type the given text into an input field matching the selector."
)

click_tool = StructuredTool.from_function(
    func=click,
    name="click",
    description="(session_id, selector) → Click the first element matching the selector."
)

extract_tool = StructuredTool.from_function(
    func=extract,
    name="extract",
    description="(session_id, selector) → Extract the innerText of elements matching the selector."
)

remember_tool = StructuredTool.from_function(
    func=remember,
    name="remember",
    description="(key, value) → Store value under the given key in memory. Accepts string, list, or dict."
)

recall_tool = StructuredTool.from_function(
    func=recall,
    name="recall",
    description="(key) → Retrieve previously stored value for a given key."
)
