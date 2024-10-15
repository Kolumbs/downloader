"""Asynchronous API with FastAPI."""

import asyncio
import logging
from typing import List, Optional

from fastapi import FastAPI
import pydantic


from .stream import DownloadStream

app = FastAPI()

log = logging.getLogger(__name__)


class DownloadFilesRequest(pydantic.BaseModel):
    """Request model for the download_files endpoint."""

    files_to_download: List[str]
    url: str
    retries: int = 10


class DownloadFilesResponse(pydantic.BaseModel):
    """Response model for the download_files endpoint."""

    success: bool
    failed_attempts: int
    last_error: Optional[str] = None


async def execute_download_request(
    stream: DownloadStream, url: str, file: str, failures=10
):
    """Download a single file."""
    failed_attempts = 0
    last_error = None
    success = True
    while True:
        try:
            await stream.download(url + file, file)
            break
        except Exception as e:
            await asyncio.sleep(2)
            failed_attempts += 1
            log.warning(f"Encounter failure for {file}: {e}")
            if failed_attempts >= failures:
                last_error = str(e)
                success = False
                break
    return {
        "success": success,
        "failed_attempts": failed_attempts,
        "last_error": last_error,
    }


@app.post("/download_files")
async def download_files(request: DownloadFilesRequest) -> DownloadFilesResponse:
    """Download files from internet and save them to server disk."""
    stream = DownloadStream()
    tasks = []
    url = request.url if request.url.endswith("/") else request.url + "/"
    for file in request.files_to_download:
        tasks.append(
            execute_download_request(stream, url, file, request.retries)
        )
    results = await asyncio.gather(*tasks)
    try:
        last_error = next(
            i["last_error"] for i in results if i.get("last_error")
        )
    except StopIteration:
        last_error = None
    return {
        "success": all(i["success"] is True for i in results),
        "failed_attempts": sum(
            i["failed_attempts"] for i in results if i.get("failed_attempts", 0)
        ),
        "last_error": last_error,
    }
