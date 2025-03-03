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



# # Storage for job metadata (in production, use a database)
# JOBS_STORE: Dict[str, Dict[str, Any]] = {}
# OUTPUT_DIR = settings.DEFAULT_OUTPUT_ROOT
# os.makedirs(OUTPUT_DIR, exist_ok=True)


# class ProcessRequest(BaseModel):
#     input_file: str
#     parameters: Optional[Dict[str, Any]] = {}


# class JobResponse(BaseModel):
#     job_id: str
#     status: str
#     task_id: Optional[str] = None
#     result: Optional[Dict[str, Any]] = None


# @router.get("/jobs", response_model=JobResponse)
# async def create_processing_job(
#     file_path: str,
#     remove_attribute: list[str] | None = None,
#     remove_all_attributes: bool = False,
#     remove_color: bool = False,
#     format: str | None = None,
#     line: int | None = None,
#     returns: int | None = None,
#     number: int | None = None,
#     density: float | None = None,
#     roi: str | None = None,
#     outcrs: str | None = None,
#     incrs: str | None = None,
# ):
#     """Start an asynchronous processing job using Celery"""
#     # Generate a unique job ID
#     job_id = str(uuid.uuid4())

#     # Convert query params to PointCloudRequest model
#     request = PointCloudRequest(
#         file_path=file_path,
#         remove_attribute=remove_attribute,
#         remove_all_attributes=remove_all_attributes,
#         remove_color=remove_color,
#         format=format,
#         line=line,
#         returns=returns,
#         number=number,
#         density=density,
#         roi=tuple(map(float, roi.split(","))) if roi else None,
#         outcrs=outcrs,
#         incrs=incrs,
#     )

#     cli_args = request.to_cli_arguments()     
#     # Store job metadata
#     JOBS_STORE[job_id] = {
#         "task_id": task.id,
#         "status": "PENDING",
#         "input_file": file_path,
#         "parameters": cli_args,
#         "created_at": time.time(),
#         "output_file": None,
#     }

#     return JobResponse(job_id=job_id, status="PENDING", task_id=task.id)


# @router.get("/jobs/{job_id}", response_model=JobResponse)
# async def get_job_status(job_id: str):
#     """Get the status of an asynchronous processing job"""
#     if job_id not in JOBS_STORE:
#         raise HTTPException(status_code=404, detail="Job not found")

#     # Get the stored job information
#     job_info = JOBS_STORE[job_id]

#     # Get the Celery task status
#     task_id = job_info["task_id"]
#     task_result = AsyncResult(task_id, app=celery_app)

#     # Update the job status
#     status = task_result.status
#     JOBS_STORE[job_id]["status"] = status

#     # If the task is successful, extract the output filename
#     result = "None"
#     if status == "SUCCESS" and task_result.result:
#         result = task_result.result
#         if isinstance(result, dict) and result.get("output_file"):
#             JOBS_STORE[job_id]["output_file"] = result.get("output_file")

#     return JobResponse(job_id=job_id, status=status, task_id=task_id, result=result)


# @router.get("/jobs/{job_id}/result")
# async def get_job_result(job_id: str):
#     """Get the result file of a completed processing job"""
#     if job_id not in JOBS_STORE:
#         raise HTTPException(status_code=404, detail="Job not found")

#     job = JOBS_STORE[job_id]

#     if job["status"] != "SUCCESS":
#         raise HTTPException(
#             status_code=400,
#             detail=f"Job not completed successfully. Current status: {job['status']}",
#         )

#     if not job["output_file"]:
#         raise HTTPException(status_code=404, detail="Output file not found")

#     file_path = os.path.join(OUTPUT_DIR, job["output_file"])
#     if not os.path.exists(file_path):
#         raise HTTPException(status_code=404, detail="File not found")

#     return FileResponse(
#         path=file_path,
#         filename=job["output_file"],
#         media_type="application/octet-stream",
#     )
