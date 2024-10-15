"""Stream module for downloading files from internet."""

import asyncio
import logging
import os
import socket
import urllib.parse as urlparse


log = logging.getLogger(__name__)


class SingleFile:
    """Single file local stream."""

    def __init__(self, file_name):
        """Initialize file stream.

        :param file_name: Name of the file.
        """
        self.file_name = file_name
        self.size = None
        self._fn = None

    async def open(self):
        """Open file for reading."""
        if self._fn is not None:
            raise RuntimeError("File already open")
        if os.path.exists(self.file_name):
            self._fn = await asyncio.to_thread(open, self.file_name, mode="ab")
            self.size = os.path.getsize(self.file_name)
        else:
            self._fn = await asyncio.to_thread(open, self.file_name, mode="wb")
            self.size = 0

    async def close(self):
        """Close file."""
        if self._fn is None:
            raise RuntimeError("File not open")
        await asyncio.to_thread(self._fn.close)
        self._fn = None

    async def write(self, data: bytes):
        """Write data to file."""
        if self._fn is None:
            raise RuntimeError("File not open")
        await asyncio.to_thread(self._fn.write, data)


class DownloadStream:
    """Allows to safely download files from internet.

    By safely mostly meant recovery of partially downloaded content after network
    errors.
    """

    def __init__(self, cache_dir=".", chunk_size=65536 * 100):
        """Initialize downloader.

        :param cache_dir: Directory to store temporary files in.
        """
        self.cache_dir = cache_dir
        self.chunk_size = chunk_size

    async def _get_reader_writer(
        self, host: str, port: int
    ) -> (asyncio.StreamReader, asyncio.StreamWriter):
        """Get reader and writer for URL.

        :param url: URL to get reader and writer for.
        :return: Tuple of reader and writer.

        Remember to close writer after you are done with it.
        """
        try:
            return await asyncio.wait_for(
                asyncio.open_connection(
                    host=host,
                    port=port,
                    ssl=True,
                    ssl_handshake_timeout=1,
                ),
                timeout=3,
            )
        except asyncio.TimeoutError:
            raise RuntimeError(f"Connection to host {host} at {port} timed out")
        except socket.gaierror:
            raise RuntimeError(f"Failed to resolve host: {host}")

    async def _send_http_request(
        self,
        writer: asyncio.StreamWriter,
        host: str,
        path: str,
        method: str = "GET",
        connection: str = "close",
        headers: dict = {},
    ) -> None:
        """Send HTTP request to server.

        :param writer: Writer to send request to.
        """
        writer.write(
            method.encode("utf-8")
            + b" "
            + path.encode("utf-8")
            + b" HTTP/1.1\r\n"
        )
        writer.write(b"Host: " + host.encode("utf-8") + b"\r\n")
        writer.write(b"Connection: " + connection.encode("utf-8") + b"\r\n")
        for key, value in headers.items():
            writer.write(
                key.encode("utf-8") + b": " + value.encode("utf-8") + b"\r\n"
            )
        writer.write(b"\r\n")
        await writer.drain()

    async def _receive_http_header(self, reader: asyncio.StreamReader) -> dict:
        """Receive HTTP header from server.

        :param reader: Reader to receive header from.
        :return: Dictionary of header fields or raises RuntimeError.
        """
        header = {}
        headline = ""
        while True:
            try:
                line = await asyncio.wait_for(reader.readline(), timeout=3)
            except asyncio.TimeoutError:
                raise RuntimeError("Timeout while receiving valid HTTP header.")
            if not line.strip():
                break
            if not headline:
                headline = line.decode("utf-8")
                headz = headline.split(" ", 3)
                if len(headz) < 3:
                    raise RuntimeError(
                        f"Invalid HTTP header received {headline}"
                    )
                if headz[0] != "HTTP/1.1":
                    raise RuntimeError(f"Unsupported HTTP version {headz[0]}")
                if headz[1] not in ["200", "206"]:
                    raise RuntimeError(
                        f"Expected status code in 200 or 206 got {headz[1]}"
                    )
            else:
                key, value = line.decode("utf-8").split(":", 1)
                header[key.strip()] = value.strip()
        if "Content-Length" not in header:
            raise RuntimeError("Host did not send Content-Length header")
        return header

    async def _get_file_from_cache(self, file_name: str) -> SingleFile:
        """Retrieve file from local cache."""
        cached_file = SingleFile(file_name + ".cache")
        await cached_file.open()
        return cached_file

    async def download(self, url: str, file_name: str):
        """Downloads file from internet and saves it.

        :param url: URL to download file from.
        :param file_name: Local file name to save downloaded file to.

        Succesfully saves file or raises RuntimeError, after saving cache.
        """
        parsed_url = urlparse.urlparse(url)
        if parsed_url.scheme not in ("https"):
            raise RuntimeError("Only HTTPS is supported")
        host = parsed_url.hostname
        port = parsed_url.port or 443
        path = (
            parsed_url.path
            if parsed_url.path.endswith("/")
            else parsed_url.path + "/"
        )
        reader, writer = await self._get_reader_writer(host, port)
        await self._send_http_request(writer, host, path, "HEAD", "keep-alive")
        try:
            header = await self._receive_http_header(reader)
        except RuntimeError as e:
            writer.close()
            await writer.wait_closed()
            raise RuntimeError(
                f"Host {host} at port {port} did not send valid HTTP header: {e}"
            )
        if "Accept-Ranges" not in header or header["Accept-Ranges"] != "bytes":
            raise RuntimeError("Host does not support partial download")
        current = await self._get_file_from_cache(file_name)
        size = int(header["Content-Length"])
        # We fetch only if cache is not up to date
        if current.size != size:
            headers = {"Range": f"bytes={current.size}-{size-1}"}
            await self._send_http_request(
                writer, host, path, "GET", "close", headers
            )
            try:
                header = await self._receive_http_header(reader)
                chunk_size = int(header["Content-Length"])
                if current.size + chunk_size != size:
                    raise RuntimeError("Host did not send requested range")
                while current.size != size:
                    chunk = (
                        size - current.size
                        if size - current.size < self.chunk_size
                        else self.chunk_size
                    )
                    byte_chunk = await asyncio.wait_for(
                        reader.readexactly(chunk), timeout=3
                    )
                    await current.write(byte_chunk)
                    current.size += len(byte_chunk)
                    log.warning(
                        "Downloading %s: %.2f%%", path, current.size / size * 100
                    )
            finally:
                await current.close()
                writer.close()
                try:
                    await asyncio.wait_for(writer.wait_closed(), timeout=3)
                except asyncio.TimeoutError:
                    pass
        # Move cache to final location
        os.rename(current.file_name, current.file_name[:-6])
