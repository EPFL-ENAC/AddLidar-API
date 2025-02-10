from fastapi import FastAPI
from app.routers import process_point_cloud
from app.internal import admin

app = FastAPI()

# Include routers
app.include_router(process_point_cloud.router, prefix="/api")
app.include_router(admin.router, prefix="/admin", tags=["admin"])