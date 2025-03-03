TODO:

INFO: Uvicorn running on http://0.0.0.0:8081 (Press CTRL+C to quit)
output/output_533a4ba67249491d80d353e8760cfae9.bin
INFO: 10.95.34.78:49024 - "GET /health HTTP/1.1" 200 OK
INFO: 10.95.34.78:34626 - "GET /health HTTP/1.1" 200 OK
INFO: 10.95.34.78:36282 - "GET /health HTTP/1.1" 200 OK
INFO: 10.233.114.128:51512 - "GET /process-point-cloud?file_path=%2FLiDAR%2F0001_Mission_Root%2F02_LAS_PCD%2Fall_grouped_high_veg_10th_point.las&outcrs=EPSG%3A4326&format=pcd-ascii HTTP/1.1" 500 Internal Server Error
ERROR: Exception in ASGI application
Traceback (most recent call last):
File "/app/.venv/lib/python3.12/site-packages/starlette/responses.py", line 341, in __call__
stat_result = await anyio.to_thread.run_sync(os.stat, self.path)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/app/.venv/lib/python3.12/site-packages/anyio/to_thread.py", line 56, in run_sync
return await get_async_backend().run_sync_in_worker_thread(
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/app/.venv/lib/python3.12/site-packages/anyio/_backends/_asyncio.py", line 2461, in run_sync_in_worker_thread
return await future
^^^^^^^^^^^^
File "/app/.venv/lib/python3.12/site-packages/anyio/_backends/_asyncio.py", line 962, in run
result = context.run(func, *args)
^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: '/data/output/output_533a4ba67249491d80d353e8760cfae9.bin'
During handling of the above exception, another exception occurred:
Traceback (most recent call last):
File "/app/.venv/lib/python3.12/site-packages/uvicorn/protocols/http/h11_impl.py", line 403, in run_asgi
result = await app( # type: ignore[func-returns-value]
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/app/.venv/lib/python3.12/site-packages/uvicorn/middleware/proxy_headers.py", line 60, in __call__
return await self.app(scope, receive, send)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/app/.venv/lib/python3.12/site-packages/fastapi/applications.py", line 1054, in __call__
await super().__call__(scope, receive, send)
File "/app/.venv/lib/python3.12/site-packages/starlette/applications.py", line 112, in __call__
await self.middleware_stack(scope, receive, send)
File "/app/.venv/lib/python3.12/site-packages/starlette/middleware/errors.py", line 187, in __call__
raise exc
File "/app/.venv/lib/python3.12/site-packages/starlette/middleware/errors.py", line 165, in __call__
await self.app(scope, receive, _send)
File "/app/.venv/lib/python3.12/site-packages/starlette/middleware/exceptions.py", line 62, in __call__
await wrap_app_handling_exceptions(self.app, conn)(scope, receive, send)
File "/app/.venv/lib/python3.12/site-packages/starlette/_exception_handler.py", line 53, in wrapped_app
raise exc
File "/app/.venv/lib/python3.12/site-packages/starlette/_exception_handler.py", line 42, in wrapped_app
await app(scope, receive, sender)
File "/app/.venv/lib/python3.12/site-packages/starlette/routing.py", line 715, in __call__
await self.middleware_stack(scope, receive, send)
File "/app/.venv/lib/python3.12/site-packages/starlette/routing.py", line 735, in app
await route.handle(scope, receive, send)
File "/app/.venv/lib/python3.12/site-packages/starlette/routing.py", line 288, in handle
await self.app(scope, receive, send)
File "/app/.venv/lib/python3.12/site-packages/starlette/routing.py", line 76, in app
await wrap_app_handling_exceptions(app, request)(scope, receive, send)
File "/app/.venv/lib/python3.12/site-packages/starlette/_exception_handler.py", line 53, in wrapped_app
raise exc
File "/app/.venv/lib/python3.12/site-packages/starlette/_exception_handler.py", line 42, in wrapped_app
await app(scope, receive, sender)
File "/app/.venv/lib/python3.12/site-packages/starlette/routing.py", line 74, in app
await response(scope, receive, send)
File "/app/.venv/lib/python3.12/site-packages/starlette/responses.py", line 344, in __call__
raise RuntimeError(f"File at path {self.path} does not exist.")
RuntimeError: File at path /data/output/output_533a4ba67249491d80d353e8760cfae9.bin does not exist.
INFO: 10.95.34.78:34248 - "GET /health HTTP/1.1" 200 OK
INFO: 10.95.34.78:41682 - "GET /health HTTP/1.1" 200 OK
INFO: 10.95.34.78:58276 - "GET /health HTTP/1.1" 200 OK
INFO: 10.95.34.78:57232 - "GET /health HTTP/1.1" 200 OK