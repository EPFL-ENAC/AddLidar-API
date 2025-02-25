1) fastapi.response FileResponse
2) stream directly 
````
import aiofiles
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

app = FastAPI()

async def async_file_reader(file_path):
    async with aiofiles.open(file_path, 'rb') as file:
        while chunk := await file.read(1024):
            yield chunk

@app.get("/async-download")
async def async_download_file():
    file_path = "large_file.zip"
    return StreamingResponse(async_file_reader(file_path), media_type="application/octet-stream")
```

3) make the API a job api