"""
---------------------------------------------
Main file for creating fastapi.FastAPI object
---------------------------------------------
"""
from contextlib import asynccontextmanager
import time

from fastapi import FastAPI, Request, status
from fastapi.responses import RedirectResponse

from servers.src.api.app.routers import (
    chat,
    health,
)
from servers.src.api.app.shared_state import initialize_assistant_graph
from servers.src.api.config.app import DocumentationSettings

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await initialize_assistant_graph()
    yield


app = FastAPI(**DocumentationSettings().model_dump(), lifespan=lifespan)


# route to redirect root URL to /docs
@app.get("/", include_in_schema=False)  # exclude this route from the generated documentation
def redirect_to_docs():
    """Redirect to documentation"""
    return RedirectResponse(url="/docs", status_code=status.HTTP_302_FOUND)


@app.middleware("http")
async def add_process_time_header(request: Request, call_next: callable):
    """Middleware to add the X-Process-Time header to the HTTP response."""
    start_time = time.perf_counter()

    response = await call_next(request)
    process_time = time.perf_counter() - start_time

    response.headers["X-Process-Time"] = f"{round(process_time, 3)} sec"
    return response


# add routers
app.include_router(health.router)
app.include_router(chat.router)
