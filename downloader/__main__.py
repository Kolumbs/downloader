"""Command line utility to download files from internet."""

import argparse
import asyncio
import logging
import time

from stream import DownloadStream

log = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download files from internet.")
    parser.add_argument("url", help="URL to download file from")
    parser.add_argument("output", help="Output file name")
    args = parser.parse_args()

    downloader = DownloadStream()
    start = time.time()
    asyncio.run(downloader.download(args.url, args.output))
    print(f"Downloaded {args.url} in {time.time()-start:.2f}s")
