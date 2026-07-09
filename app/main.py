import os
import logging
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from mcp.server.sse import SseServerTransport

from app.config import settings
from app.mcp_server import mcp_server, MCPToolHandler

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("appoptimize.main")

sse = SseServerTransport("/messages")

# Application state containing shared httpx client
state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages application lifecycle and shared HTTP client connection pool."""
    logger.info("Initializing AppOptimize MCP Server...")
    http_client = httpx.AsyncClient(timeout=600.0)
    state["http_client"] = http_client
    state["tool_handler"] = MCPToolHandler(http_client=http_client)
    yield
    logger.info("Shutting down AppOptimize MCP Server...")
    await http_client.aclose()


app = FastAPI(
    title="AppOptimize MCP Server",
    version="0.2.0",
    description="MCP server for Google AppOptimize API and BigQuery",
    lifespan=lifespan,
)


@app.get("/health")
@app.get("/livez")
async def health_check():
    """Health check endpoint for Cloud Run and Kubernetes probers."""
    return {
        "status": "ok",
        "project_id": settings.project_id or "not_configured",
        "version": "0.2.0",
    }


@app.get("/sse")
async def sse_endpoint():
    """SSE endpoint for Model Context Protocol (MCP) clients."""
    async with sse.connect_sse() as (read_stream, write_stream):
        await mcp_server.run(
            read_stream,
            write_stream,
            mcp_server.create_initialization_options(),
        )


@app.post("/messages")
async def messages_endpoint(request: Request):
    """SSE message handler endpoint for MCP."""
    await sse.handle_post_message(request)


@app.post("/call/{name}")
async def call_tool_direct(name: str, arguments: dict = None):
    """Direct REST endpoint for calling MCP tools without SSE transport."""
    tool_handler: MCPToolHandler = state.get("tool_handler") or MCPToolHandler()
    try:
        results = await tool_handler.execute(name, arguments or {})
        return JSONResponse(content={"status": "success", "result": results})
    except Exception as e:
        logger.exception(f"Error executing tool '{name}' via direct REST endpoint")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)},
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app" if __name__ != "__main__" else app,
        host=settings.host,
        port=settings.port,
        reload=False,
    )
