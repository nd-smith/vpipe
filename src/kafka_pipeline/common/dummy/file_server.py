"""
Dummy file server for serving test attachments.

Provides a simple HTTP server that generates realistic-looking files
based on the requested path and file type.
"""

import asyncio
import hashlib
import struct
from dataclasses import dataclass
from typing import Optional

from aiohttp import web

from core.logging.setup import get_logger

logger = get_logger(__name__)


@dataclass
class FileServerConfig:
    """Configuration for the dummy file server."""
    host: str = "0.0.0.0"
    port: int = 8765
    default_file_size: int = 100_000  # 100KB default
    max_file_size: int = 10_000_000  # 10MB max


def generate_dummy_jpeg(size_bytes: int, seed: str) -> bytes:
    """
    Generate a valid JPEG file of approximately the specified size.

    Creates a minimal valid JPEG structure with filler data.
    The content is deterministic based on the seed.
    """
    # JPEG header (SOI + APP0 JFIF marker)
    header = bytes([
        0xFF, 0xD8,  # SOI (Start of Image)
        0xFF, 0xE0,  # APP0 marker
        0x00, 0x10,  # Length of APP0 segment (16 bytes)
        0x4A, 0x46, 0x49, 0x46, 0x00,  # "JFIF\0"
        0x01, 0x01,  # Version 1.1
        0x00,  # Aspect ratio units (0 = no units)
        0x00, 0x01,  # X density
        0x00, 0x01,  # Y density
        0x00, 0x00,  # No thumbnail
    ])

    # Minimal quantization table (required)
    quant_table = bytes([0xFF, 0xDB, 0x00, 0x43, 0x00]) + bytes([8] * 64)

    # Minimal Huffman table (required)
    huffman_dc = bytes([
        0xFF, 0xC4, 0x00, 0x1F, 0x00,
        0x00, 0x01, 0x05, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B,
    ])

    # SOF0 (Start of Frame) - defines a 16x16 image
    sof = bytes([
        0xFF, 0xC0,  # SOF0 marker
        0x00, 0x0B,  # Length
        0x08,  # Precision (8 bits)
        0x00, 0x10,  # Height (16)
        0x00, 0x10,  # Width (16)
        0x01,  # Number of components
        0x01, 0x11, 0x00,  # Component 1: Y, sampling 1x1, quant table 0
    ])

    # SOS (Start of Scan) marker
    sos = bytes([
        0xFF, 0xDA,  # SOS marker
        0x00, 0x08,  # Length
        0x01,  # Number of components
        0x01, 0x00,  # Component 1, DC/AC table 0/0
        0x00, 0x3F, 0x00,  # Spectral selection
    ])

    # EOI (End of Image)
    footer = bytes([0xFF, 0xD9])

    # Calculate padding needed
    fixed_size = len(header) + len(quant_table) + len(huffman_dc) + len(sof) + len(sos) + len(footer)
    padding_size = max(0, size_bytes - fixed_size - 4)  # 4 bytes for comment marker

    # Generate deterministic padding based on seed
    seed_hash = hashlib.sha256(seed.encode()).digest()
    padding = bytes((seed_hash[i % len(seed_hash)] for i in range(padding_size)))

    # Wrap padding in a comment marker (COM)
    if padding_size > 0:
        comment_length = min(padding_size + 2, 65535)
        comment = bytes([0xFF, 0xFE]) + struct.pack(">H", comment_length) + padding[:comment_length - 2]
    else:
        comment = b""

    return header + quant_table + huffman_dc + sof + comment + sos + footer


def generate_dummy_pdf(size_bytes: int, seed: str) -> bytes:
    """
    Generate a valid PDF file of approximately the specified size.

    Creates a minimal valid PDF structure with filler content.
    """
    # Generate deterministic content
    seed_hash = hashlib.sha256(seed.encode()).hexdigest()

    header = b"%PDF-1.4\n"

    # Catalog object
    catalog = b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"

    # Pages object
    pages = b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n"

    # Page object
    page = b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R >>\nendobj\n"

    # Calculate content stream size
    fixed_size = len(header) + len(catalog) + len(pages) + len(page) + 200  # ~200 for xref/trailer
    content_size = max(100, size_bytes - fixed_size)

    # Generate content stream with filler text
    content_text = f"BT /F1 12 Tf 100 700 Td (Test Document - {seed_hash[:16]}) Tj ET\n"
    padding_size = max(0, content_size - len(content_text) - 50)

    # Add padding as PDF comments
    padding_lines = []
    line_content = f"% Padding: {seed_hash}"
    while len("\n".join(padding_lines)) < padding_size:
        padding_lines.append(line_content)
    padding = "\n".join(padding_lines)[:padding_size]

    full_content = (content_text + padding).encode()
    content_stream = f"4 0 obj\n<< /Length {len(full_content)} >>\nstream\n".encode()
    content_stream += full_content
    content_stream += b"\nendstream\nendobj\n"

    # Calculate offsets for xref
    offset1 = len(header)
    offset2 = offset1 + len(catalog)
    offset3 = offset2 + len(pages)
    offset4 = offset3 + len(page)

    # Cross-reference table
    xref = f"xref\n0 5\n0000000000 65535 f \n{offset1:010d} 00000 n \n{offset2:010d} 00000 n \n{offset3:010d} 00000 n \n{offset4:010d} 00000 n \n".encode()

    # Trailer
    startxref = len(header) + len(catalog) + len(pages) + len(page) + len(content_stream)
    trailer = f"trailer\n<< /Size 5 /Root 1 0 R >>\nstartxref\n{startxref}\n%%EOF".encode()

    return header + catalog + pages + page + content_stream + xref + trailer


def generate_dummy_file(file_type: str, size_bytes: int, seed: str) -> tuple[bytes, str]:
    """
    Generate a dummy file of the specified type and size.

    Returns (content_bytes, content_type).
    """
    file_type = file_type.lower()

    if file_type in ("jpg", "jpeg"):
        return generate_dummy_jpeg(size_bytes, seed), "image/jpeg"
    elif file_type == "pdf":
        return generate_dummy_pdf(size_bytes, seed), "application/pdf"
    elif file_type == "png":
        # Minimal valid PNG (1x1 transparent pixel)
        png_header = bytes([
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,  # PNG signature
        ])
        # IHDR chunk
        ihdr = bytes([
            0x00, 0x00, 0x00, 0x0D,  # Length
            0x49, 0x48, 0x44, 0x52,  # "IHDR"
            0x00, 0x00, 0x00, 0x01,  # Width: 1
            0x00, 0x00, 0x00, 0x01,  # Height: 1
            0x08, 0x06,  # 8-bit RGBA
            0x00, 0x00, 0x00,  # Compression, filter, interlace
            0x1F, 0x15, 0xC4, 0x89,  # CRC
        ])
        # Minimal IDAT
        idat = bytes([
            0x00, 0x00, 0x00, 0x0A,  # Length
            0x49, 0x44, 0x41, 0x54,  # "IDAT"
            0x78, 0x9C, 0x62, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,  # Compressed data
            0x00, 0x05, 0xFE, 0x02,  # CRC (placeholder)
        ])
        # IEND
        iend = bytes([
            0x00, 0x00, 0x00, 0x00,
            0x49, 0x45, 0x4E, 0x44,
            0xAE, 0x42, 0x60, 0x82,
        ])
        base = png_header + ihdr + idat + iend
        # Pad with text chunks if needed
        padding_needed = max(0, size_bytes - len(base))
        if padding_needed > 0:
            seed_bytes = (seed * ((padding_needed // len(seed)) + 1))[:padding_needed].encode()
            # Add as tEXt chunk (not ideal but makes file larger)
            base = png_header + ihdr + idat + seed_bytes + iend
        return base[:size_bytes] if len(base) > size_bytes else base, "image/png"
    else:
        # Generic binary file
        seed_hash = hashlib.sha256(seed.encode()).digest()
        content = bytes((seed_hash[i % len(seed_hash)] for i in range(size_bytes)))
        return content, "application/octet-stream"


class DummyFileServer:
    """
    HTTP server that serves dummy files for pipeline testing.

    Routes:
        GET /files/{project_id}/{media_id}/{filename} - Download a generated file
        GET /health - Health check endpoint
    """

    def __init__(self, config: Optional[FileServerConfig] = None):
        self.config = config or FileServerConfig()
        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None

    async def start(self) -> None:
        """Start the HTTP server."""
        self._app = web.Application()
        self._app.router.add_get("/files/{project_id}/{media_id}/{filename}", self._handle_file_download)
        self._app.router.add_get("/health", self._handle_health)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()

        self._site = web.TCPSite(
            self._runner,
            self.config.host,
            self.config.port,
        )
        await self._site.start()

        logger.info(
            "Dummy file server started",
            extra={"host": self.config.host, "port": self.config.port},
        )

    async def stop(self) -> None:
        """Stop the HTTP server."""
        if self._runner:
            await self._runner.cleanup()
            logger.info("Dummy file server stopped")

    async def __aenter__(self) -> "DummyFileServer":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.stop()

    async def _handle_file_download(self, request: web.Request) -> web.Response:
        """Handle file download requests."""
        project_id = request.match_info["project_id"]
        media_id = request.match_info["media_id"]
        filename = request.match_info["filename"]

        # Extract file type from filename
        file_type = filename.split(".")[-1] if "." in filename else "bin"

        # Get requested size from query param, or use default
        try:
            size = int(request.query.get("size", self.config.default_file_size))
            size = min(size, self.config.max_file_size)
        except ValueError:
            size = self.config.default_file_size

        # Generate deterministic seed from path
        seed = f"{project_id}/{media_id}/{filename}"

        # Generate the file
        content, content_type = generate_dummy_file(file_type, size, seed)

        logger.debug(
            "Serving dummy file",
            extra={
                "project_id": project_id,
                "media_id": media_id,
                "filename": filename,
                "size": len(content),
                "content_type": content_type,
            },
        )

        return web.Response(
            body=content,
            content_type=content_type,
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Length": str(len(content)),
            },
        )

    async def _handle_health(self, request: web.Request) -> web.Response:
        """Health check endpoint."""
        return web.json_response({"status": "healthy", "service": "dummy-file-server"})


async def run_file_server(config: Optional[FileServerConfig] = None) -> None:
    """Run the file server as a standalone service."""
    async with DummyFileServer(config) as server:
        # Run until interrupted
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
