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


- http://0.0.0.0:8000/process-point-cloud?file_path=%2FLiDAR%2F0001_Mission_Root%2F02_LAS_PCD%2Fall_grouped_high_veg_10th_point.las&outcrs=EPSG%3A4326&format=pcd-ascii

-https://addlidar-potree-dev.epfl.ch/api/process-point-cloud?file_path=%2FLiDAR%2F0001_Mission_Root%2F02_LAS_PCD%2Fall_grouped_high_veg_10th_point.las&outcrs=EPSG%3A4326&format=pcd-ascii

- http://localhost:8081/process-point-cloud?file_path=%2FLiDAR%2F0001_Mission_Root%2F02_LAS_PCD%2Fall_grouped_high_veg_10th_point.las&outcrs=EPSG%3A4326&format=pcd-ascii