# Advanced Sitemap Parser

An advanced XML sitemap processor built for large-scale URL extraction, capable of bypassing most modern anti-bot protection systems. It supports plain XML and compressed XML files (.xml.gz), along with unlimited levels of nested/child sitemaps. It can fetch sitemaps directly from URLs, from a file containing multiple sitemap URLs, or from a local directory of `.xml` and `.xml.gz` files.

Also features a number of optional and advanced settings such as dynamic proxy and user agent rotation, CloudScraper integration, fingerprint randomization, auto stealth mode, and includes detailed logging and monitoring.

## Key Features

### Anti-Detection System

- **Human-like browsing patterns** with randomized delays (configurable 3-30+ seconds)
- **Intelligent retry logic** with exponential backoff for 403/429 errors
- **Real-time proxy rotation** - switches IP for every single request
- **Dynamic user agent rotation** - changes browser fingerprint per request
- **CloudScraper integration** with browser fingerprint randomization
- **Enhanced HTTP headers** mimicking real browser behavior (Sec-CH-UA, DNT, etc.)
- **Request timing variation** with occasional longer "human-like" pauses
- **Stealth mode** for maximum evasion with extended delays
- **Advanced fingerprint randomization** without browser overhead

### Multi-Threading with Intelligent Staggering

- **Configurable concurrency** from 1 (stealth) to unlimited workers
- **Automatic request staggering** (0.5-2 second delays between thread starts)
- **Thread-safe processing** with proper interrupt handling
- **Sequential fallback** for maximum stealth operations

### Proxy System

- **Multiple proxy formats supported:**
  - `ip:port` (basic HTTP proxy)
  - `ip:port:username:password` (authenticated proxy)
  - `http://ip:port` (full URL format)
  - `http://username:password@ip:port` (authenticated URL format)
- **Automatic proxy rotation** - never reuses the same IP consecutively
- **Proxy health monitoring** with real-time IP display
- **External proxy file support** - load thousands of proxies from text file
- **Fallback to direct connection** if no proxies provided

### User Agent Management

- **Built-in user agent database** with current browser versions
- **External user agent file support** for custom UA lists
- **Per-request rotation** ensuring maximum diversity
- **Real-time UA display** showing current browser fingerprint
- **Browser-specific header matching** (Chrome, Firefox, Safari, Edge)
- **CloudScraper override** forcing custom user agents

### Sitemap Processing Engine

- **Unlimited nested sitemap support** - processes sitemap indexes recursively
- **Compressed file handling** - automatic .gz decompression
- **XML namespace awareness** - proper sitemap standard compliance
- **Duplicate URL prevention** - intelligent deduplication system
- **Progress monitoring** with real-time queue size and URL counts
- **Error recovery** - continues processing despite individual failures
- **Failed URL tracking** with detailed error reporting

### Input/Output Management

- **Multiple input methods:**
  - Single sitemap URL (`--url`)
  - Batch processing from file (`--file`)
  - Directory scanning for local `.xml` and `.xml.gz` files (`--directory`)
- **Configurable output directory** (`--save-dir`)
- **Smart filename generation** with readable source hints plus a short stability hash
- **Organized output files** with metadata headers
- **Failed URL recovery files** for easy reprocessing
- **Summary reporting** with processing statistics
- **Comprehensive logging** to `sitemap_processing.log`

### Monitoring & Error Handling

- **Graceful interrupt handling** - Ctrl+C stops processing cleanly
- **Session persistence** through connection errors
- **Automatic retry mechanisms** with configurable limits
- **Memory efficient** processing of large sitemaps
- **Real-time progress reporting** with timestamps
- **Detailed error tracking** and retry statistics
- **Cross-platform compatibility** (Linux, macOS, Windows)

## Installation

Supported Python: 3.9+

```bash
# Clone the repository
git clone https://github.com/phase3dev/advanced-sitemap-parser.git
cd advanced-sitemap-parser

# Install dependencies
pip install -r requirements.txt
```

## CloudScraper Notice

CloudScraper has an upstream memory leak issue tracked in [VeNoMouS/cloudscraper issue #328](https://github.com/VeNoMouS/cloudscraper/issues/328). In CloudScraper `3.0.0`, `rotate_tls_ciphers=True` can leak memory by mounting a new TLS adapter on every request without closing the old one.

This project uses `cloudscraper.create_scraper(...)`, so long-running or high-volume CloudScraper runs may be affected depending on the installed CloudScraper version and behavior. This is an upstream CloudScraper issue, not a bug in `advanced-sitemap-parser`.

Current workaround from the issue:

```python
scraper = cloudscraper.create_scraper(rotate_tls_ciphers=False)
```

If you observe growing memory usage during long CloudScraper runs, test with `rotate_tls_ciphers=False` and rotate sessions externally if needed. If upstream fixes the issue in a later release, this note can be revisited.

## Quick Start

### Basic Usage

```bash
# Simple sitemap extraction
python3 sitemap_extract.py --url https://example.com/sitemap.xml

# With output directory
python3 sitemap_extract.py --url https://example.com/sitemap.xml --save-dir ./results
```

### Stealth Mode

```bash
# Maximum stealth with longer delays
python3 sitemap_extract.py --url https://example.com/sitemap.xml --stealth

# Stealth with proxy rotation
python3 sitemap_extract.py --url https://example.com/sitemap.xml --stealth --proxy-file proxies.txt
```

### Proxy Rotation

```bash
# Basic proxy rotation
python3 sitemap_extract.py --url https://example.com/sitemap.xml --proxy-file proxies.txt

# Proxy + custom user agents
python3 sitemap_extract.py --url https://example.com/sitemap.xml --proxy-file proxies.txt --user-agent-file user_agents.txt
```

### Multi-Threading

```bash
# Moderate concurrency (3 threads with staggering)
python3 sitemap_extract.py --url https://example.com/sitemap.xml --max-workers 3 --proxy-file proxies.txt

# High-speed processing (use with caution)
python3 sitemap_extract.py --url https://example.com/sitemap.xml --max-workers 10 --min-delay 1 --max-delay 3
```

### Batch Processing

```bash
# Process multiple sitemaps from file
python3 sitemap_extract.py --file sitemap_urls.txt --proxy-file proxies.txt --stealth

# Reprocess failed URLs
python3 sitemap_extract.py --file failed_sitemap_urls.txt --proxy-file proxies.txt --max-retries 5
```

### Timing Controls

```bash
# Custom delays for specific sites
python3 sitemap_extract.py --url https://example.com/sitemap.xml --min-delay 5 --max-delay 15 --max-retries 5

# Fast processing (less stealthy)
python3 sitemap_extract.py --url https://example.com/sitemap.xml --min-delay 0.5 --max-delay 2 --max-workers 5
```

### Complete Command Reference

```bash
python3 sitemap_extract.py [OPTIONS]

# Input Options
--url URL                    Direct URL of sitemap file
--file FILE                  File containing list of sitemap URLs
--directory DIR              Directory containing .xml and .xml.gz files

# Output Options
--save-dir DIR               Directory to save all output files (default: current)

# Anti-Detection Options
--proxy-file FILE            File containing proxy list (see format below)
--user-agent-file FILE       File containing user agent list
--stealth                    Maximum evasion mode (5-12s delays, forces --max-workers=1)
--no-cloudscraper            Use standard requests instead of CloudScraper

# Performance Options
--max-workers N              Concurrent workers (default: 1 for max stealth)
--min-delay SECONDS          Minimum delay between requests (default: 3.0)
--max-delay SECONDS          Maximum delay between requests (default: 8.0)
--max-retries N              Maximum retry attempts per URL (default: 3)
```

## File Formats

### Proxy File Format (proxies.txt)

```bash
# Basic format
192.168.1.100:8080
proxy.example.com:3128

# With authentication
192.168.1.101:8080:username:password

# Full URL format
http://proxy.example.com:8080
http://user:pass@proxy.example.com:8080
```

### User Agent File Format (user_agents.txt)

```bash
# One user agent per line
Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36
Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36
Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0
```

### Sitemap URL File Format (sitemap_urls.txt)

```bash
# One URL per line
https://www.example.com/sitemap.xml
https://www.example.com/sitemap_index.xml
https://www.example.com/sitemaps/sitemap.xml.gz
```

## Output Files

### Individual Sitemap Files

- **Format:** `domain_com_hint_<short-hash>.txt`
- **Contains:** Deduplicated page URLs from that specific sitemap source only
- **Metadata:** Source URL, generation timestamp, URL count
- **Readability:** Remote files include the host plus path/query hints when available, and local files include the parent directory plus file stem
- **Uniqueness:** The short hash is derived from the full original source string, so query-distinct child sitemap URLs do not overwrite each other
- **Example:** `www_example_com_sitemap_sitemap1_<short-hash>.txt`

### Merged URL File

- **`all_extracted_urls.txt`:** Deduplicated union of all extracted page URLs from the run, with the same metadata header format as per-sitemap files

### Summary Files

- **`all_sitemaps_summary.log`:** Complete inventory of all sitemaps processed
- **`failed_sitemap_urls.txt`:** Clean list of failed URLs for reprocessing
- **`sitemap_processing.log`:** Detailed processing log with timestamps

### Failed URL Recovery

When sitemaps fail to process, they're automatically saved to `failed_sitemap_urls.txt` for easy reprocessing:

```bash
# Reprocess failed URLs with different settings
python3 sitemap_extract.py --file failed_sitemap_urls.txt --proxy-file proxies.txt --max-retries 5
```

## Real-Time Monitoring

The script provides comprehensive real-time feedback:

```bash
[2025-08-29 05:52:39] Loaded 1000 proxies from proxies.txt
[2025-08-29 05:52:39] Loaded 1000 user agents from user_agents.txt
[2025-08-29 05:52:39] Max concurrent workers: 3
[2025-08-29 05:52:39] Press Ctrl+C to stop gracefully...
[2025-08-29 05:52:39] Fetching (attempt 1): https://example.com/sitemap.xml
[2025-08-29 05:52:39] Using IP: 192.168.1.100
[2025-08-29 05:52:39] Using User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)...
[2025-08-29 05:52:40] SUCCESS with 192.168.1.100
[2025-08-29 05:52:40] Processed: 15 nested sitemaps, 1,250 pages
[2025-08-29 05:52:40] Saved 1,250 URLs to example_com_sitemap_xml.txt
```

## Advanced Configuration

### Stealth Mode Behavior

When `--stealth` is enabled:

- Minimum delay increased to 5+ seconds
- Maximum delay increased to 12+ seconds
- `--max-workers` is forced to `1`
- All other anti-detection features activated

### Threading with Staggering

Multi-threading includes automatic staggering to avoid simultaneous requests:

- 0.5-2 second delays between thread starts
- Each thread maintains individual timing
- Stealth mode disables multi-worker execution by forcing a single worker

### Error Handling

- **403 Forbidden**: Exponential backoff (5-15+ seconds)
- **429 Rate Limited**: Extended delays (20-40 seconds)
- **Timeouts**: Progressive retry delays
- **Network errors**: Automatic retry with different proxy/UA

## Troubleshooting

### Common Issues

- **All requests getting 403**: Try stealth mode with longer delays
- **Proxy errors**: Verify proxy format and authentication
- **Memory issues**: Reduce max-workers or process smaller batches
- **Ctrl+C not working**: Script includes proper interrupt handling

### Getting Help

- Check the log file: `sitemap_processing.log`
- Review failed URLs: `failed_sitemap_urls.txt`

### Effectiveness & Limitations

This script purposely employs an HTTP-based approach, providing an optimal balance between stealth and efficiency. It's been tested on sites with millions of URLs and successfully bypasses protection on most sitemap sources.

However, sites with advanced protection mechanisms (JavaScript challenges, CAPTCHA systems, behavioral analysis) will still require full browser automation tools.

## Testing

```bash
python -m unittest test_sitemap_extract.py
```

Covers interruptible sleep behavior, proxy/IP formatting, interrupt propagation, concurrent stats and failure tracking, and a threaded local sitemap run.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

MIT License - see LICENSE file for details
