from fastapi import FastAPI, Request
from src.api.routes import router
from fastapi.responses import JSONResponse
from pydantic import ValidationError

app = FastAPI()

@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError):
    return JSONResponse(
        status_code=422,
        content={
            "detail": exc.errors(),
            "body": exc.model.model_dump() if exc.model else None
        }
    )


app.include_router(router)