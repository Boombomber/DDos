import asyncio
import statistics
import time
import threading
import random
import re
from collections import Counter
from typing import List, Optional

import aiohttp
from aiohttp_socks import ProxyConnector
import tkinter as tk
from tkinter.scrolledtext import ScrolledText
from tkinter import ttk

TARGET_URL_DEFAULT = "https://deine-domain.de"
CONCURRENCY_DEFAULT = 10
TOTAL_REQUESTS_DEFAULT = 100
TIMEOUT_SECS_DEFAULT = 10.0
RETRY_COUNT_DEFAULT = 0
RETRY_DELAY_DEFAULT = 0.2

stats_lock = threading.Lock()
log_lock = threading.Lock()

text_area: ScrolledText | None = None
proxy_text: tk.Text | None = None
entry_target_url: ttk.Entry | None = None
entry_concurrency: ttk.Entry | None = None
entry_total_requests: ttk.Entry | None = None
entry_timeout_secs: ttk.Entry | None = None
entry_retry_count: ttk.Entry | None = None
entry_retry_delay: ttk.Entry | None = None
use_proxies_var: tk.BooleanVar | None = None

class LoadStats:
    def __init__(self):
        self.running = False
        self.stop_requested = False
        self.total = 0
        self.successful = 0
        self.failed = 0
        self.durations: List[float] = []
        self.status_counts: Counter = Counter()
        self.error_counts: Counter = Counter()
        self.start_time = 0.0
        self.end_time = 0.0

    def reset(self):
        self.running = False
        self.stop_requested = False
        self.total = 0
        self.successful = 0
        self.failed = 0
        self.durations.clear()
        self.status_counts.clear()
        self.error_counts.clear()
        self.start_time = 0.0
        self.end_time = 0.0

    def record_success(self, duration: float, status: int):
        with stats_lock:
            self.total += 1
            self.successful += 1
            self.durations.append(duration)
            self.status_counts[status] += 1

    def record_failure(self, duration: float, error: str):
        with stats_lock:
            self.total += 1
            self.failed += 1
            self.durations.append(duration)
            self.error_counts[error] += 1

    def rps(self) -> float:
        if self.start_time == 0 or self.end_time == 0:
            return 0.0
        delta = self.end_time - self.start_time
        return self.total / delta if delta > 0 else 0.0

    def quantile(self, q: float) -> float:
        if not self.durations:
            return 0.0
        if len(self.durations) == 1:
            return self.durations[0]
        idx = max(0, min(99, int(q * 99)))
        return statistics.quantiles(self.durations, n=100)[idx]

    def summary(self) -> str:
        if not self.durations:
            return "Keine Daten."
        mean = statistics.mean(self.durations)
        median = statistics.median(self.durations)
        p95 = self.quantile(0.95)
        p99 = self.quantile(0.99)
        success_rate = self.successful / self.total * 100 if self.total else 0.0
        lines = [
            f"Total requests: {self.total}",
            f"Successful: {self.successful} ({success_rate:.1f}%)",
            f"Failed: {self.failed}",
            f"Duration: {self.end_time - self.start_time:.2f}s",
            f"RPS: {self.rps():.1f}",
            f"Mean: {mean:.3f}s, Median: {median:.3f}s",
            f"P95: {p95:.3f}s, P99: {p99:.3f}s",
        ]
        if self.status_counts:
            lines.append("Status distribution:")
            for s, c in sorted(self.status_counts.items()):
                lines.append(f"  {s}: {c}")
        if self.error_counts:
            lines.append("Error distribution:")
            for e, c in sorted(self.error_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
                lines.append(f"  {e}: {c}")
        return "\n".join(lines)

stats = LoadStats()

def log(msg: str):
    with log_lock:
        if text_area is not None:
            text_area.after(0, lambda m=msg: _log_gui(m))
        else:
            print(msg)

def _log_gui(msg: str):
    if text_area is not None:
        text_area.insert("end", msg + "\n")
        text_area.see("end")

def normalize_proxy_line(line: str) -> Optional[str]:
    line = line.strip()
    if not line:
        return None
    m = re.search(r'\((https?://[^)]+)\)', line)
    if m:
        line = m.group(1).strip()
    if line.startswith("http://") or line.startswith("socks4://") or line.startswith("socks5://"):
        return line
    return None

def get_proxy_list() -> List[str]:
    if proxy_text is None:
        return []
    raw = proxy_text.get("1.0", "end-1c")
    seen = set()
    out = []
    for line in raw.splitlines():
        p = normalize_proxy_line(line)
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    return out

def filter_proxy_text():
    if proxy_text is None:
        return
    raw = proxy_text.get("1.0", "end-1c")
    seen = set()
    cleaned = []
    removed = 0
    for line in raw.splitlines():
        p = normalize_proxy_line(line)
        if not p or p in seen:
            removed += 1
            continue
        seen.add(p)
        cleaned.append(p)
    proxy_text.delete("1.0", "end")
    proxy_text.insert("1.0", "\n".join(cleaned))
    log(f"Proxy-Filter: {len(cleaned)} behalten, {removed} entfernt.")

def get_entry_int(widget, default):
    try:
        return int(widget.get().strip())
    except Exception:
        return default

def get_entry_float(widget, default):
    try:
        return float(widget.get().strip())
    except Exception:
        return default

def get_entry_str(widget, default=""):
    try:
        return widget.get().strip()
    except Exception:
        return default

def is_socks_proxy(proxy_url: str) -> bool:
    return proxy_url.startswith("socks4://") or proxy_url.startswith("socks5://")

async def request_once(session: aiohttp.ClientSession, url: str, timeout_secs: float, proxy_url: Optional[str]):
    if proxy_url:
        if is_socks_proxy(proxy_url):
            connector = ProxyConnector.from_url(proxy_url)
            async with aiohttp.ClientSession(connector=connector) as proxy_session:
                async with proxy_session.get(url, timeout=timeout_secs) as resp:
                    return resp.status
        else:
            async with session.get(url, timeout=timeout_secs, proxy=proxy_url) as resp:
                return resp.status
    async with session.get(url, timeout=timeout_secs) as resp:
        return resp.status

async def worker(url: str, timeout_secs: float, retry_count: int, retry_delay: float,
                 proxy_list: List[str], use_proxies: bool, concurrency_sem: asyncio.Semaphore):
    async with concurrency_sem:
        while stats.running and not stats.stop_requested:
            with stats_lock:
                if stats.total >= TARGET_REQUESTS_CURRENT:
                    return
            start = time.perf_counter()
            error = None
            status = None
            tried = 0
            proxy_url = random.choice(proxy_list) if use_proxies and proxy_list else None
            while tried <= retry_count and not stats.stop_requested:
                tried += 1
                try:
                    async with aiohttp.ClientSession() as session:
                        status = await request_once(session, url, timeout_secs, proxy_url)
                    if status >= 500 and tried <= retry_count:
                        await asyncio.sleep(retry_delay)
                        continue
                    break
                except asyncio.TimeoutError:
                    if tried <= retry_count:
                        await asyncio.sleep(retry_delay)
                        continue
                    error = "timeout"
                except Exception as e:
                    error = f"{type(e).__name__}: {e}"
                    break
            duration = time.perf_counter() - start
            if error:
                stats.record_failure(duration, error)
                log(f"[ERR] {error}")
            else:
                stats.record_success(duration, status if status is not None else 0)
                log(f"[{status}] {duration:.3f}s")

async def run_loadtest_async(url: str, concurrency: int, total_requests: int, timeout_secs: float,
                             retry_count: int, retry_delay: float, proxy_list: List[str], use_proxies: bool):
    global TARGET_REQUESTS_CURRENT
    TARGET_REQUESTS_CURRENT = total_requests
    stats.reset()
    stats.running = True
    stats.start_time = time.perf_counter()
    log("=== Loadtest gestartet ===")
    log(f"URL: {url}")
    log(f"Concurrency: {concurrency}, Total: {total_requests}")
    log(f"Timeout: {timeout_secs}s, Retries: {retry_count}, RetryDelay: {retry_delay}s")
    log(f"Proxies: {'an' if use_proxies else 'aus'} | {len(proxy_list)} Einträge")
    sem = asyncio.Semaphore(concurrency)
    tasks = [worker(url, timeout_secs, retry_count, retry_delay, proxy_list, use_proxies, sem)
             for _ in range(concurrency)]
    try:
        await asyncio.gather(*tasks)
    finally:
        stats.end_time = time.perf_counter()
        stats.running = False
        log("=== Loadtest beendet ===")
        log(stats.summary())

def start_loadtest_gui():
    if stats.running:
        log("Loadtest läuft bereits.")
        return
    url = get_entry_str(entry_target_url, TARGET_URL_DEFAULT)
    concurrency = max(1, get_entry_int(entry_concurrency, CONCURRENCY_DEFAULT))
    total_requests = max(1, get_entry_int(entry_total_requests, TOTAL_REQUESTS_DEFAULT))
    timeout_secs = max(0.1, get_entry_float(entry_timeout_secs, TIMEOUT_SECS_DEFAULT))
    retry_count = max(0, get_entry_int(entry_retry_count, RETRY_COUNT_DEFAULT))
    retry_delay = max(0.0, get_entry_float(entry_retry_delay, RETRY_DELAY_DEFAULT))
    proxy_list = get_proxy_list()
    use_proxies = bool(use_proxies_var.get()) if use_proxies_var is not None else False
    if use_proxies and not proxy_list:
        log("Keine gültigen Proxies gefunden.")
        return
    log("=== Loadtest wird gestartet ===")
    thread = threading.Thread(
        target=lambda: asyncio.run(
            run_loadtest_async(url, concurrency, total_requests, timeout_secs, retry_count, retry_delay, proxy_list, use_proxies)
        ),
        daemon=True
    )
    thread.start()

def stop_loadtest_gui():
    stats.stop_requested = True
    log("Stop-Signal gesendet.")

def build_gui():
    global text_area, proxy_text, entry_target_url, entry_concurrency, entry_total_requests
    global entry_timeout_secs, entry_retry_count, entry_retry_delay, use_proxies_var

    root = tk.Tk()
    root.title("Loadtester mit Proxy-Filter")

    main = ttk.Frame(root, padding=10)
    main.pack(fill="both", expand=True)

    settings = ttk.Frame(main)
    settings.pack(fill="x", pady=5)

    ttk.Label(settings, text="URL:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
    entry_target_url = ttk.Entry(settings, width=50)
    entry_target_url.grid(row=0, column=1, sticky="we", padx=5, pady=2)
    entry_target_url.insert(0, TARGET_URL_DEFAULT)

    ttk.Label(settings, text="Concurrency:").grid(row=1, column=0, sticky="w", padx=5, pady=2)
    entry_concurrency = ttk.Entry(settings, width=10)
    entry_concurrency.grid(row=1, column=1, sticky="w", padx=5, pady=2)
    entry_concurrency.insert(0, str(CONCURRENCY_DEFAULT))

    ttk.Label(settings, text="Total Requests:").grid(row=2, column=0, sticky="w", padx=5, pady=2)
    entry_total_requests = ttk.Entry(settings, width=10)
    entry_total_requests.grid(row=2, column=1, sticky="w", padx=5, pady=2)
    entry_total_requests.insert(0, str(TOTAL_REQUESTS_DEFAULT))

    ttk.Label(settings, text="Timeout (s):").grid(row=3, column=0, sticky="w", padx=5, pady=2)
    entry_timeout_secs = ttk.Entry(settings, width=10)
    entry_timeout_secs.grid(row=3, column=1, sticky="w", padx=5, pady=2)
    entry_timeout_secs.insert(0, str(TIMEOUT_SECS_DEFAULT))

    ttk.Label(settings, text="Retry Count:").grid(row=4, column=0, sticky="w", padx=5, pady=2)
    entry_retry_count = ttk.Entry(settings, width=10)
    entry_retry_count.grid(row=4, column=1, sticky="w", padx=5, pady=2)
    entry_retry_count.insert(0, str(RETRY_COUNT_DEFAULT))

    ttk.Label(settings, text="Retry Delay (s):").grid(row=5, column=0, sticky="w", padx=5, pady=2)
    entry_retry_delay = ttk.Entry(settings, width=10)
    entry_retry_delay.grid(row=5, column=1, sticky="w", padx=5, pady=2)
    entry_retry_delay.insert(0, str(RETRY_DELAY_DEFAULT))

    proxy_frame = ttk.Frame(main)
    proxy_frame.pack(fill="both", expand=True, pady=5)

    ttk.Label(proxy_frame, text="Proxies (eine pro Linie):").grid(row=0, column=0, sticky="w", padx=5, pady=2)
    proxy_text = tk.Text(proxy_frame, width=90, height=15)
    proxy_text.grid(row=1, column=0, columnspan=3, sticky="nsew", padx=5, pady=2)

    use_proxies_var = tk.BooleanVar(value=False)
    ttk.Checkbutton(proxy_frame, text="Proxies verwenden", variable=use_proxies_var).grid(row=2, column=0, sticky="w", padx=5, pady=2)
    ttk.Button(proxy_frame, text="Proxies filtern", command=filter_proxy_text).grid(row=2, column=1, sticky="e", padx=5, pady=2)

    proxy_frame.columnconfigure(0, weight=1)
    proxy_frame.rowconfigure(1, weight=1)

    buttons = ttk.Frame(main)
    buttons.pack(fill="x", pady=5)

    ttk.Button(buttons, text="Start Loadtest", command=start_loadtest_gui).pack(side="left", padx=5)
    ttk.Button(buttons, text="Stop", command=stop_loadtest_gui).pack(side="left", padx=5)

    text_area = ScrolledText(main, width=100, height=20)
    text_area.pack(fill="both", expand=True, padx=5, pady=5)

    log("GUI bereit.")
    log("Proxy-Formate: http://ip:port, socks4://ip:port, socks5://ip:port")
    root.mainloop()

if __name__ == "__main__":
    build_gui()