from fastapi import APIRouter, Depends
from app.dependencies import get_token_header

router = APIRouter(
    dependencies=[Depends(get_token_header)],
    responses={404: {"description": "Not found"}},
)

@router.get("/")
async def read_admin():
    return {"message": "Admin getting schwifty"}