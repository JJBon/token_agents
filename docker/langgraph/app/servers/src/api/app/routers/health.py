import httpx
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from servers.src.variables import OLLAMA_API_URL

router = APIRouter(prefix='/health', tags=['Health Check'])


@router.get('/check-service-status')
async def check_service_status() -> JSONResponse:
    """
    Health check endpoint to verify that the FastAPI service is running.

    **Returns:**

    - `JSONResponse`: A 200 OK response indicating the FastAPI service is healthy.
    """
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"status": "healthy"}
    )


@router.get('/check-ollama-status')
async def check_ollama_status() -> JSONResponse:
    """
    Health check endpoint to verify that the Ollama server is reachable and responsive.

    **Returns:**

    - `JSONResponse`: 200 OK if the Ollama server is reachable and responsive. 503 Service Unavailable if the Ollama
    server is unreachable or returns an error.
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(OLLAMA_API_URL)
            if response.status_code == status.HTTP_200_OK:
                return JSONResponse(
                    status_code=status.HTTP_200_OK,
                    content={"status": "ok", "ollama": "running"}
                )
            else:
                return JSONResponse(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    content={"status": "error", "ollama": "unreachable"}
                )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "error", "ollama": "not running", "detail": str(e)}
        )
