import logging
import os
import shutil
import sys
import textwrap
import threading
import time
import types
import unittest
from pathlib import Path
from unittest import mock

sys.modules.setdefault(
    "cloudscraper",
    types.SimpleNamespace(create_scraper=lambda **kwargs: None),
)

import sitemap_extract


class ExplodingProxy:
    def get(self, key, default=None):
        raise KeyboardInterrupt()


class SitemapExtractHardeningTests(unittest.TestCase):
    def make_processor(self, **kwargs):
        processor = sitemap_extract.HumanizedSitemapProcessor(
            use_cloudscraper=False,
            save_dir=".",
            **kwargs,
        )
        processor.print_status = lambda message: None
        return processor

    def test_interruptible_sleep_exits_promptly(self):
        processor = self.make_processor()

        def interrupt():
            time.sleep(0.05)
            processor.interrupted = True

        interrupter = threading.Thread(target=interrupt)
        interrupter.start()

        start = time.monotonic()
        with self.assertRaises(KeyboardInterrupt):
            processor.interruptible_sleep(1.0)
        elapsed = time.monotonic() - start

        interrupter.join()
        self.assertLess(elapsed, 0.5)

    def test_get_current_ip_formats_proxy_types_and_preserves_keyboard_interrupt(self):
        processor = self.make_processor()

        self.assertEqual(processor.get_current_ip(), "Direct Connection")
        self.assertEqual(
            processor.get_current_ip({"http": "http://10.20.30.40:8080"}),
            "10.20.30.40",
        )
        self.assertEqual(
            processor.get_current_ip({"http": "http://user:pass@10.20.30.41:8080"}),
            "10.20.30.41",
        )

        with self.assertRaises(KeyboardInterrupt):
            processor.get_current_ip(ExplodingProxy())

    def test_locked_state_helpers_are_exact_under_concurrency(self):
        processor = self.make_processor()
        worker_count = 8
        increments_per_worker = 250

        def worker(index):
            for _ in range(increments_per_worker):
                processor.increment_stat("retries")
                processor.increment_stat("errors")
                processor.increment_stat("pages_found", 2)
            processor.record_failed_url(
                f"https://example.com/failure-{index}.xml",
                "boom",
                status_code=500,
                attempts=3,
            )

        threads = [
            threading.Thread(target=worker, args=(index,))
            for index in range(worker_count)
        ]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        snapshot = processor.get_state_snapshot()
        self.assertEqual(
            snapshot["session_stats"]["retries"],
            worker_count * increments_per_worker,
        )
        self.assertEqual(
            snapshot["session_stats"]["errors"],
            worker_count * increments_per_worker,
        )
        self.assertEqual(
            snapshot["session_stats"]["pages_found"],
            worker_count * increments_per_worker * 2,
        )
        self.assertEqual(len(snapshot["failed_urls"]), worker_count)

    def test_threaded_local_run_keeps_summary_counts_stable(self):
        tmp_path = Path(os.getcwd()) / "test_tmp_threaded_local_run"
        if tmp_path.exists():
            shutil.rmtree(tmp_path)
        tmp_path.mkdir()

        try:
            input_dir = tmp_path / "input"
            output_dir = tmp_path / "output"
            input_dir.mkdir()
            output_dir.mkdir()

            (input_dir / "root.xml").write_text(
                textwrap.dedent(
                    """\
                    <?xml version="1.0" encoding="UTF-8"?>
                    <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                      <sitemap><loc>child1.xml</loc></sitemap>
                      <sitemap><loc>child2.xml</loc></sitemap>
                    </sitemapindex>
                    """
                ),
                encoding="utf-8",
            )
            (input_dir / "child1.xml").write_text(
                textwrap.dedent(
                    """\
                    <?xml version="1.0" encoding="UTF-8"?>
                    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                      <url><loc>https://example.com/page-1</loc></url>
                      <url><loc>https://example.com/page-2</loc></url>
                    </urlset>
                    """
                ),
                encoding="utf-8",
            )
            (input_dir / "child2.xml").write_text(
                textwrap.dedent(
                    """\
                    <?xml version="1.0" encoding="UTF-8"?>
                    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                      <url><loc>https://example.com/page-3</loc></url>
                    </urlset>
                    """
                ),
                encoding="utf-8",
            )

            processor = sitemap_extract.HumanizedSitemapProcessor(
                use_cloudscraper=False,
                max_workers=3,
                save_dir=str(output_dir),
            )
            processor.print_status = lambda message: None

            with mock.patch("sitemap_extract.random.uniform", return_value=0.0):
                with mock.patch("sitemap_extract.signal.signal"):
                    all_sitemap_urls, all_page_urls = processor.process_all_sitemaps(
                        [str(input_dir / "root.xml")]
                    )

            snapshot = processor.get_state_snapshot()
            self.assertEqual(snapshot["session_stats"]["sitemaps_processed"], 3)
            self.assertEqual(snapshot["session_stats"]["pages_found"], 3)
            self.assertEqual(snapshot["session_stats"]["errors"], 0)
            self.assertEqual(snapshot["session_stats"]["retries"], 0)
            self.assertEqual(len(snapshot["failed_urls"]), 0)
            self.assertEqual(len(all_sitemap_urls), 3)
            self.assertEqual(
                all_page_urls,
                {
                    "https://example.com/page-1",
                    "https://example.com/page-2",
                    "https://example.com/page-3",
                },
            )
            self.assertTrue((output_dir / "all_extracted_urls.txt").exists())
        finally:
            logging.shutdown()
            shutil.rmtree(tmp_path)


if __name__ == "__main__":
    unittest.main()
