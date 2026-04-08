import os
import xml.etree.ElementTree as ET
import gzip
import logging
import argparse
import cloudscraper
import random
import hashlib
import re
from datetime import datetime
import sys
from urllib.parse import urljoin, urlparse
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Logging will be configured dynamically in main()

# Enhanced user agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
]

BROWSER_HEADERS = [
    {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Cache-Control": "max-age=0",
    },
    {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    },
]

FILENAME_HASH_LENGTH = 10
SLEEP_CHUNK_SECONDS = 0.1


def canonicalize_save_dir(save_dir):
    """Resolve the output directory once to a canonical absolute path."""
    return os.path.abspath(save_dir or ".")


def is_remote_source(source):
    """Return True when the source is an HTTP(S) URL."""
    return urlparse(source).scheme in ("http", "https")


def is_compressed_source(source):
    """Return True when the source path ends with .xml.gz."""
    parsed_source = urlparse(source)
    if is_remote_source(source):
        return parsed_source.path.lower().endswith(".xml.gz")
    return source.lower().endswith(".xml.gz")


def sanitize_filename_component(value):
    """Convert arbitrary text into a readable filesystem-safe filename part."""
    sanitized = re.sub(r"[^A-Za-z0-9_-]+", "_", value)
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    return sanitized


def build_output_filename(source):
    """Build a readable, collision-resistant filename from the full source."""
    parsed_source = urlparse(source)

    if is_remote_source(source):
        readable_parts = [parsed_source.netloc.replace(".", "_")]
        path_part = parsed_source.path.strip("/")
        readable_parts.append(path_part.replace("/", "_") if path_part else "root")
    else:
        local_name = os.path.basename(os.path.abspath(source))
        readable_parts = [local_name]

    readable_base = sanitize_filename_component("_".join(readable_parts)) or "sitemap"
    source_hash = hashlib.sha256(source.encode("utf-8")).hexdigest()[
        :FILENAME_HASH_LENGTH
    ]
    return f"{readable_base}_{source_hash}.txt"


def scan_sitemap_directory(directory):
    """Return only explicit .xml and .xml.gz files from a directory."""
    sitemap_files = []
    with os.scandir(directory) as entries:
        for entry in entries:
            if not entry.is_file():
                continue

            entry_name = entry.name.lower()
            if entry_name.endswith(".xml") or entry_name.endswith(".xml.gz"):
                sitemap_files.append(os.path.abspath(entry.path))

    return sorted(sitemap_files)


class HumanizedSitemapProcessor:
    def __init__(
        self,
        use_cloudscraper=True,
        proxy_file=None,
        user_agent_file=None,
        min_delay=2.0,
        max_delay=5.0,
        max_retries=3,
        max_workers=1,
        save_dir=None,
    ):
        self.use_cloudscraper = use_cloudscraper
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.max_workers = max_workers
        self.processed_urls = set()
        self.interrupted = False
        self.save_dir = canonicalize_save_dir(save_dir)
        self.processed_urls_lock = threading.Lock()
        self.state_lock = threading.Lock()
        self.request_pacing_lock = threading.Lock()

        # Enhanced failure tracking
        self.failed_urls = (
            {}
        )  # {url: {'error': 'description', 'status_code': code, 'attempts': n}}

        # Load proxies and user agents from files
        self.proxies = self.load_proxies(proxy_file) if proxy_file else []
        self.custom_user_agents = (
            self.load_user_agents(user_agent_file) if user_agent_file else []
        )

        # Use custom user agents if available, otherwise fallback to built-in
        self.user_agents = (
            self.custom_user_agents if self.custom_user_agents else USER_AGENTS
        )

        self.session_stats = {
            "sitemaps_processed": 0,
            "pages_found": 0,
            "errors": 0,
            "retries": 0,
            "start_time": time.time(),
        }
        self.last_request_time = 0

    def load_proxies(self, proxy_file):
        """Load proxies from file"""
        try:
            with open(proxy_file, "r") as f:
                proxies = []
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        # Support formats: ip:port, ip:port:user:pass, http://ip:port
                        if line.startswith("http"):
                            proxies.append({"http": line, "https": line})
                        elif ":" in line:
                            parts = line.split(":")
                            if len(parts) == 2:  # ip:port
                                proxy_url = f"http://{line}"
                                proxies.append({"http": proxy_url, "https": proxy_url})
                            elif len(parts) == 4:  # ip:port:user:pass
                                ip, port, user, password = parts
                                proxy_url = f"http://{user}:{password}@{ip}:{port}"
                                proxies.append({"http": proxy_url, "https": proxy_url})
                self.print_status(f"Loaded {len(proxies)} proxies from {proxy_file}")
                return proxies
        except Exception as e:
            self.print_status(f"Error loading proxies: {str(e)}")
            return []

    def load_user_agents(self, ua_file):
        """Load user agents from file"""
        try:
            with open(ua_file, "r") as f:
                user_agents = [
                    line.strip()
                    for line in f
                    if line.strip() and not line.startswith("#")
                ]
            self.print_status(f"Loaded {len(user_agents)} user agents from {ua_file}")
            return user_agents
        except Exception as e:
            self.print_status(f"Error loading user agents: {str(e)}")
            return []

    def get_current_ip(self, proxy=None):
        """Get current IP address for monitoring"""
        try:
            if proxy:
                proxy_str = str(proxy.get("http", "Unknown"))
                if "@" in proxy_str:
                    return proxy_str.split("@")[1].split(":")[0]
                else:
                    return proxy_str.replace("http://", "").split(":")[0]
            else:
                return "Direct Connection"
        except Exception:
            return "Unknown"

    def print_status(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")
        sys.stdout.flush()

    def interruptible_sleep(self, duration):
        """Sleep in short chunks so interrupts are handled promptly."""
        if duration <= 0:
            if self.interrupted:
                raise KeyboardInterrupt()
            return

        end_time = time.time() + duration
        while time.time() < end_time:
            if self.interrupted:
                raise KeyboardInterrupt()

            remaining = end_time - time.time()
            if remaining <= 0:
                break

            time.sleep(min(SLEEP_CHUNK_SECONDS, remaining))

        if self.interrupted:
            raise KeyboardInterrupt()

    def increment_stat(self, stat_name, amount=1):
        """Increment a session statistic under lock."""
        with self.state_lock:
            self.session_stats[stat_name] += amount

    def record_failed_url(self, url, error, status_code=None, attempts=1):
        """Record a failed sitemap fetch/load under lock."""
        with self.state_lock:
            self.failed_urls[url] = {
                "error": error,
                "status_code": status_code,
                "attempts": attempts,
            }

    def get_state_snapshot(self):
        """Return consistent snapshots of shared mutable state."""
        with self.state_lock:
            return {
                "session_stats": dict(self.session_stats),
                "failed_urls": dict(self.failed_urls),
            }

    def try_mark_processed_url(self, url):
        """Mark a sitemap source as processed once."""
        with self.processed_urls_lock:
            if url in self.processed_urls or self.interrupted:
                return False
            self.processed_urls.add(url)
            return True

    def is_processed_url(self, url):
        """Check whether a sitemap source has already been processed."""
        with self.processed_urls_lock:
            return url in self.processed_urls

    def human_delay(self):
        """Add human-like delays between requests"""
        if self.interrupted:
            raise KeyboardInterrupt()

        with self.request_pacing_lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time

            delay = random.uniform(self.min_delay, self.max_delay)

            # 15% chance of longer pause
            if random.random() < 0.15:
                delay += random.uniform(3.0, 8.0)
                self.print_status("Taking a longer human-like break...")

            if time_since_last < delay:
                sleep_time = delay - time_since_last
                self.print_status(f"Waiting {sleep_time:.2f} seconds...")
                self.interruptible_sleep(sleep_time)

            self.last_request_time = time.time()

    def create_enhanced_scraper(self):
        """Create sophisticated scraper with rotating proxies and user agents"""
        # Rotate proxy for each request
        if self.proxies:
            current_proxy = random.choice(self.proxies)
        else:
            current_proxy = None

        # Rotate user agent for each request
        current_user_agent = random.choice(self.user_agents)

        if self.use_cloudscraper:
            scraper = cloudscraper.create_scraper(
                browser={
                    "browser": random.choice(["chrome", "firefox"]),
                    "platform": random.choice(["windows", "darwin"]),
                    "desktop": True,
                }
            )
        else:
            scraper = requests.Session()
            retry_strategy = Retry(
                total=self.max_retries,
                backoff_factor=2,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            scraper.mount("http://", adapter)
            scraper.mount("https://", adapter)

        # Set headers and FORCE our user agent (override cloudscraper)
        headers = random.choice(BROWSER_HEADERS).copy()
        headers["User-Agent"] = current_user_agent

        # Add some randomness
        if random.random() < 0.3:
            headers[
                "X-Forwarded-For"
            ] = f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"

        # Enhanced browser headers
        if random.random() < 0.5:
            headers.update(
                {
                    "Sec-CH-UA": '"Not_A Brand";v="8", "Chromium";v="120"',
                    "Sec-CH-UA-Mobile": "?0",
                    "Sec-CH-UA-Platform": '"Windows"'
                    if "Windows" in current_user_agent
                    else '"macOS"',
                }
            )

        scraper.headers.update(headers)

        # CRITICAL: Force our user agent again after all header updates
        scraper.headers["User-Agent"] = current_user_agent

        # Set proxy
        if current_proxy:
            scraper.proxies.update(current_proxy)

        return scraper, current_proxy, current_user_agent

    def fetch_with_retries(self, url, is_compressed=False):
        """Fetch URL with retries and anti-detection measures"""
        for attempt in range(self.max_retries + 1):
            if self.interrupted:
                raise KeyboardInterrupt()

            current_proxy = None
            try:
                self.human_delay()
                scraper, current_proxy, current_user_agent = (
                    self.create_enhanced_scraper()
                )

                # Enhanced monitoring output
                current_ip = self.get_current_ip(current_proxy)
                ua_display = current_user_agent
                if len(ua_display) > 60:
                    ua_display = ua_display[:60] + "..."

                self.print_status(f"Fetching (attempt {attempt + 1}): {url}")
                self.print_status(f"Using IP: {current_ip}")
                self.print_status(f"Using User-Agent: {ua_display}")

                timeout = random.uniform(15, 30)
                response = scraper.get(url, timeout=timeout, stream=is_compressed)

                if response.status_code == 200:
                    self.print_status(f"SUCCESS with {current_ip}")
                    if is_compressed:
                        with gzip.open(response.raw, "rb") as f:
                            content = f.read()
                        return ET.fromstring(content)
                    else:
                        return ET.fromstring(response.content)

                elif response.status_code == 403:
                    self.print_status(
                        f"403 Forbidden with {current_ip} - attempt {attempt + 1}/{self.max_retries + 1}"
                    )
                    if attempt < self.max_retries:
                        wait_time = (2**attempt) + random.uniform(5, 15)
                        self.print_status(
                            f"Waiting {wait_time:.2f} seconds before retry..."
                        )
                        self.interruptible_sleep(wait_time)
                        self.increment_stat("retries")
                        continue
                    else:
                        # Final attempt failed, record detailed failure
                        self.record_failed_url(
                            url,
                            f"HTTP 403 Forbidden after {self.max_retries + 1} attempts",
                            status_code=403,
                            attempts=self.max_retries + 1,
                        )

                elif response.status_code == 429:
                    self.print_status(
                        f"Rate limited with {current_ip} - attempt {attempt + 1}/{self.max_retries + 1}"
                    )
                    if attempt < self.max_retries:
                        wait_time = random.uniform(20, 40)
                        self.print_status(
                            f"Rate limit hit, waiting {wait_time:.2f} seconds..."
                        )
                        self.interruptible_sleep(wait_time)
                        self.increment_stat("retries")
                        continue
                    else:
                        # Final attempt failed, record detailed failure
                        self.record_failed_url(
                            url,
                            f"HTTP 429 Rate Limited after {self.max_retries + 1} attempts",
                            status_code=429,
                            attempts=self.max_retries + 1,
                        )

                else:
                    self.print_status(
                        f"HTTP {response.status_code} with {current_ip} - attempt {attempt + 1}/{self.max_retries + 1}"
                    )
                    if attempt < self.max_retries:
                        wait_time = random.uniform(3, 8)
                        self.interruptible_sleep(wait_time)
                        self.increment_stat("retries")
                        continue
                    else:
                        # Final attempt failed, record detailed failure
                        self.record_failed_url(
                            url,
                            f"HTTP {response.status_code} after {self.max_retries + 1} attempts",
                            status_code=response.status_code,
                            attempts=self.max_retries + 1,
                        )

            except KeyboardInterrupt:
                raise
            except Exception as e:
                current_ip = self.get_current_ip(current_proxy)
                error_msg = str(e)
                self.print_status(
                    f"Error with {current_ip}: {error_msg} - attempt {attempt + 1}/{self.max_retries + 1}"
                )
                if attempt < self.max_retries:
                    self.interruptible_sleep(random.uniform(5, 10))
                    self.increment_stat("retries")
                    continue
                else:
                    # Final attempt failed, record detailed failure
                    if "timeout" in error_msg.lower():
                        error_description = (
                            f"Timeout after {self.max_retries + 1} attempts"
                        )
                    else:
                        error_description = (
                            f"{error_msg} after {self.max_retries + 1} attempts"
                        )

                    self.record_failed_url(
                        url,
                        error_description,
                        status_code=None,
                        attempts=self.max_retries + 1,
                    )

        logging.error(f"Failed to fetch {url} after {self.max_retries + 1} attempts")
        self.increment_stat("errors")
        return None

    def load_local_sitemap(self, path, is_compressed=False):
        """Load and parse a local sitemap file."""
        try:
            open_file = gzip.open if is_compressed else open
            with open_file(path, "rb") as handle:
                return ET.fromstring(handle.read())
        except Exception as e:
            error_msg = str(e)
            self.record_failed_url(path, error_msg, status_code=None, attempts=1)
            self.increment_stat("errors")
            logging.error(f"Failed to load local sitemap {path}: {error_msg}")
            self.print_status(f"Failed to load local sitemap {path}: {error_msg}")
            return None

    def load_sitemap_root(self, source):
        """Load a sitemap root from either a remote URL or a local file."""
        is_compressed = is_compressed_source(source)
        if is_remote_source(source):
            return self.fetch_with_retries(source, is_compressed)
        return self.load_local_sitemap(source, is_compressed)

    def resolve_child_sitemap_source(self, parent_source, child_source):
        """Resolve nested sitemap references for remote URLs and local files."""
        child_source = child_source.strip()
        if not child_source:
            return None

        if is_remote_source(parent_source):
            return urljoin(parent_source, child_source)

        if is_remote_source(child_source):
            return child_source

        if os.path.isabs(child_source):
            return os.path.abspath(child_source)

        return os.path.abspath(
            os.path.join(os.path.dirname(os.path.abspath(parent_source)), child_source)
        )

    def write_url_file(self, filepath, source_label, urls):
        """Write a sorted, deduplicated URL list with the standard metadata header."""
        unique_urls = sorted(set(urls))

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"Source URL: {source_label}\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n")
            f.write(f"Total URLs: {len(unique_urls)}\n")
            f.write("-" * 50 + "\n")
            for url in unique_urls:
                f.write(f"{url}\n")

        return unique_urls

    def save_urls(self, source_url, urls):
        """Save URLs to file"""
        if not urls:
            return

        try:
            filepath = os.path.join(self.save_dir, build_output_filename(source_url))
            unique_urls = self.write_url_file(filepath, source_url, urls)
            self.print_status(f"Saved {len(unique_urls)} URLs to {filepath}")

        except Exception as e:
            self.print_status(f"Failed to save URLs: {str(e)}")

    def save_all_extracted_urls(self, urls):
        """Always write the merged extracted URL output file."""
        filepath = os.path.join(self.save_dir, "all_extracted_urls.txt")
        source_label = "all_extracted_urls (merged from all processed sitemaps)"

        try:
            unique_urls = self.write_url_file(filepath, source_label, urls)
            self.print_status(f"Saved {len(unique_urls)} merged URLs to {filepath}")
        except Exception as e:
            self.print_status(f"Failed to save merged URLs: {str(e)}")

    def save_sitemap_summary(self, sitemap_urls):
        """Save the sitemap summary log and the failed sitemap URL list."""
        failed_urls = self.get_state_snapshot()["failed_urls"]
        filename = os.path.join(self.save_dir, "all_sitemaps_summary.log")
        all_known_urls = sorted(set(sitemap_urls) | set(failed_urls.keys()))
        successful_urls = [url for url in all_known_urls if url not in failed_urls]

        try:
            with open(filename, "w", encoding="utf-8") as f:
                f.write("Source URL: all_sitemaps_summary\n")
                f.write(f"Generated: {datetime.now().isoformat()}\n")
                f.write(f"Total URLs: {len(all_known_urls)}\n")
                f.write(f"Successful URLs: {len(successful_urls)}\n")
                f.write(f"Failed URLs: {len(failed_urls)}\n")
                f.write("-" * 50 + "\n")

                for url in successful_urls:
                    f.write(f"{url}\n")

                for failed_url, failure_info in sorted(failed_urls.items()):
                    error_detail = failure_info.get("error", "Unknown error")
                    f.write(f"{failed_url} [*{error_detail}*]\n")

            self.print_status(
                f"Saved sitemap summary: {len(successful_urls)} successful, {len(failed_urls)} failed URLs to {filename}"
            )

            if failed_urls:
                failed_filename = os.path.join(self.save_dir, "failed_sitemap_urls.txt")
                with open(failed_filename, "w", encoding="utf-8") as f:
                    f.write("# Failed sitemap URLs for reprocessing\n")
                    f.write(f"# Generated: {datetime.now().isoformat()}\n")
                    f.write(f"# Total failed URLs: {len(failed_urls)}\n")
                    f.write(
                        f"# Usage: python sitemap_extract.py --file {failed_filename}\n"
                    )
                    f.write("-" * 50 + "\n")
                    for failed_url in sorted(failed_urls.keys()):
                        f.write(f"{failed_url}\n")
                self.print_status(
                    f"Saved {len(failed_urls)} failed URLs to {failed_filename} for reprocessing"
                )
        except Exception as e:
            self.print_status(f"Failed to save sitemap summary: {str(e)}")

    def process_sitemap(self, url):
        """Process single sitemap"""
        if not self.try_mark_processed_url(url):
            return [], []

        root = self.load_sitemap_root(url)

        if root is None:
            return [], []

        sitemap_urls = []
        page_urls = []
        namespace = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        for sitemap in root.findall(".//sm:sitemap", namespace):
            loc_element = sitemap.find("sm:loc", namespace)
            if loc_element is not None and loc_element.text:
                sitemap_url = self.resolve_child_sitemap_source(url, loc_element.text)
                if sitemap_url and not self.is_processed_url(sitemap_url):
                    sitemap_urls.append(sitemap_url)

        for page in root.findall(".//sm:url", namespace):
            loc_element = page.find("sm:loc", namespace)
            if loc_element is not None and loc_element.text:
                page_urls.append(loc_element.text.strip())

        self.increment_stat("sitemaps_processed")
        self.increment_stat("pages_found", len(page_urls))

        self.print_status(
            f"Processed: {len(sitemap_urls)} nested sitemaps, {len(page_urls)} pages"
        )

        if page_urls:
            self.save_urls(url, page_urls)

        return sitemap_urls, page_urls

    def process_sitemap_delayed(self, url, initial_delay):
        """Process sitemap with initial stagger delay"""
        if initial_delay > 0:
            self.interruptible_sleep(initial_delay)
        return self.process_sitemap(url)

    def signal_handler(self, signum, frame):
        """Handle interrupt signals"""
        self.print_status("Received interrupt signal, stopping gracefully...")
        self.interrupted = True

    def process_all_sitemaps(self, start_urls):
        """Process all sitemaps with optional threading"""
        # Set up signal handler
        signal.signal(signal.SIGINT, self.signal_handler)

        queue = list(dict.fromkeys(start_urls))
        all_sitemap_urls = set(queue)
        all_page_urls = set()

        self.print_status(f"Starting processing of {len(queue)} initial sitemaps")
        self.print_status(
            f"Using delays between {self.min_delay}-{self.max_delay} seconds"
        )
        self.print_status(f"Proxies available: {len(self.proxies)}")
        self.print_status(f"User agents available: {len(self.user_agents)}")
        self.print_status(f"Max concurrent workers: {self.max_workers}")
        self.print_status("Press Ctrl+C to stop gracefully...")

        try:
            if self.max_workers == 1:
                # Sequential processing for maximum stealth
                while queue and not self.interrupted:
                    url = queue.pop(0)
                    try:
                        sitemap_urls, page_urls = self.process_sitemap(url)

                        new_sitemaps = [
                            surl
                            for surl in sitemap_urls
                            if surl not in all_sitemap_urls
                            and not self.is_processed_url(surl)
                        ]
                        queue.extend(new_sitemaps)

                        all_sitemap_urls.update(new_sitemaps)
                        all_page_urls.update(page_urls)

                        self.print_status(
                            f"Queue size: {len(queue)}, Total URLs found: {len(all_page_urls)}"
                        )

                    except KeyboardInterrupt:
                        break
                    except Exception as e:
                        logging.error(f"Error processing {url}: {str(e)}")
                        self.print_status(f"Error processing {url}: {str(e)}")
                        self.increment_stat("errors")
            else:
                # Multi-threaded processing
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    while queue and not self.interrupted:
                        # Process batch
                        current_batch = queue[: self.max_workers]
                        queue = queue[self.max_workers :]

                        # Stagger request starts to avoid simultaneous hits
                        future_to_url = {}
                        for i, url in enumerate(current_batch):
                            if self.interrupted:
                                break
                            # Stagger by 0.5-2 seconds per thread
                            delay = i * random.uniform(0.5, 2.0)
                            future = executor.submit(
                                self.process_sitemap_delayed, url, delay
                            )
                            future_to_url[future] = url

                        # Collect results
                        for future in as_completed(future_to_url):
                            if self.interrupted:
                                break
                            url = future_to_url[future]
                            try:
                                sitemap_urls, page_urls = future.result()

                                new_sitemaps = [
                                    surl
                                    for surl in sitemap_urls
                                    if surl not in all_sitemap_urls
                                    and not self.is_processed_url(surl)
                                ]
                                queue.extend(new_sitemaps)

                                all_sitemap_urls.update(new_sitemaps)
                                all_page_urls.update(page_urls)

                            except Exception as e:
                                logging.error(f"Error processing {url}: {str(e)}")
                                self.print_status(f"Error processing {url}: {str(e)}")
                                self.increment_stat("errors")

                        self.print_status(
                            f"Queue size: {len(queue)}, Total URLs found: {len(all_page_urls)}"
                        )

        except KeyboardInterrupt:
            self.print_status("Processing interrupted by user")

        self.save_all_extracted_urls(all_page_urls)
        self.save_sitemap_summary(all_sitemap_urls)

        return all_sitemap_urls, all_page_urls

    def print_summary(self, all_sitemap_urls, all_page_urls):
        """Print summary"""
        state_snapshot = self.get_state_snapshot()
        session_stats = state_snapshot["session_stats"]
        failed_urls = state_snapshot["failed_urls"]
        elapsed_time = time.time() - session_stats["start_time"]
        total_sitemaps = len(set(all_sitemap_urls) | set(failed_urls.keys()))
        successful_sitemaps = max(total_sitemaps - len(failed_urls), 0)

        self.print_status("=" * 60)
        self.print_status("PROCESSING COMPLETE")
        self.print_status("=" * 60)
        self.print_status(f"Total runtime: {elapsed_time:.2f} seconds")
        self.print_status(f"Unique sitemap URLs found: {total_sitemaps}")
        self.print_status(f"Sitemap URLs successfully processed: {successful_sitemaps}")
        self.print_status(f"Total page URLs extracted: {len(all_page_urls)}")
        if failed_urls:
            self.print_status(
                f'Sitemap URLs failed to process: {len(failed_urls)} [specific URLs listed in "failed_sitemap_urls.txt"]'
            )
        else:
            self.print_status(f"Sitemap URLs failed to process: 0")
        self.print_status(f"Errors encountered: {session_stats['errors']}")
        self.print_status(f"Retries performed: {session_stats['retries']}")


def main():
    parser = argparse.ArgumentParser(
        description="Humanized XML sitemap processor with proxy rotation"
    )
    parser.add_argument("--url", type=str, help="Direct URL of sitemap file")
    parser.add_argument("--file", type=str, help="File containing list of sitemap URLs")
    parser.add_argument(
        "--directory", type=str, help="Directory containing .xml and .xml.gz files"
    )
    parser.add_argument(
        "--save-dir",
        type=str,
        help="Directory to save all output files (default: current directory)",
    )
    parser.add_argument(
        "--no-cloudscraper",
        action="store_true",
        help="Use requests instead of CloudScraper",
    )
    parser.add_argument("--proxy-file", type=str, help="File containing proxy list")
    parser.add_argument(
        "--user-agent-file", type=str, help="File containing user agent list"
    )
    parser.add_argument(
        "--min-delay", type=float, default=3.0, help="Minimum delay (default: 3.0)"
    )
    parser.add_argument(
        "--max-delay", type=float, default=8.0, help="Maximum delay (default: 8.0)"
    )
    parser.add_argument(
        "--max-retries", type=int, default=3, help="Max retries (default: 3)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="Concurrent workers (default: 1, max stealth)",
    )
    parser.add_argument(
        "--stealth",
        action="store_true",
        help="Extra stealth mode (raises delays and forces --max-workers=1)",
    )

    args = parser.parse_args()

    if args.max_workers < 1:
        parser.error("--max-workers must be at least 1")

    save_dir = canonicalize_save_dir(args.save_dir)

    # Create save directory if specified and doesn't exist
    try:
        os.makedirs(save_dir, exist_ok=True)
        print(f"[INFO] Using save directory: {save_dir}")
    except Exception as e:
        print(f"[ERROR] Could not create save directory {save_dir}: {str(e)}")
        return 1

    # Configure logging to use save directory
    log_filepath = os.path.join(save_dir, "sitemap_processing.log")
    logging.basicConfig(
        filename=log_filepath,
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filemode="w",  # Overwrite log file each run
    )

    # Stealth adjustments
    if args.stealth:
        args.min_delay = max(args.min_delay, 5.0)
        args.max_delay = max(args.max_delay, 12.0)
        args.max_workers = 1

    # Collect URLs to process
    urls_to_process = []
    if args.url:
        urls_to_process.append(args.url)
    if args.file:
        try:
            with open(args.file, "r") as f:
                urls_to_process.extend([line.strip() for line in f if line.strip()])
        except Exception as e:
            print(f"[ERROR] Could not read file {args.file}: {str(e)}")
            return 1
    if args.directory:
        try:
            urls_to_process.extend(scan_sitemap_directory(args.directory))
        except Exception as e:
            print(f"[ERROR] Could not read directory {args.directory}: {str(e)}")
            return 1

    if not urls_to_process:
        print("[ERROR] No URLs provided")
        return 1

    # Instantiate and run processor
    processor = HumanizedSitemapProcessor(
        use_cloudscraper=not args.no_cloudscraper,
        proxy_file=args.proxy_file,
        user_agent_file=args.user_agent_file,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        max_retries=args.max_retries,
        max_workers=args.max_workers,
        save_dir=save_dir,
    )

    try:
        all_sitemap_urls, all_page_urls = processor.process_all_sitemaps(
            urls_to_process
        )
        processor.print_summary(all_sitemap_urls, all_page_urls)
        return 0
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Processing stopped by user")
        processor.print_summary(set(), set())
        return 130


if __name__ == "__main__":
    sys.exit(main())
