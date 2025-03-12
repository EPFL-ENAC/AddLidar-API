DONE Better API doc
Add some persistence for the job, by using redis

DONE Get properly the logs like in process_cloud_points for start_job

Add a kill/clean JOB name route
Add more tests

- Still errors on job creation / when volume not mounted.. error does not show

- Better status for job/pod monitoring
- Check the pod creation limit, since we don't delete the pods anymore (maybe change that)
- instead of having the cronjob running every week. we should probably run it every day
- We should probably have a backgroundTask that check the inMemory status of jobs and delete the unecessary one

- since we're having inmemory status, we chould on ping, check that the job still exist.
If not, maybe try deleting it on kube with safe failing, and then send a message and close connection

- Add an option to the start-job route to delete on job download or after 3 or 4 download (other options would be to have a background task that clean up job, and the clean up job would delete the file, not sure how to do it yet)

- When trying to create a job and hitting the max quota with start-job we return a 200 on the POST despite having the kube API returning a 403: Do something better than this!

{
  "job_name": [
    "Failed to create or run job job-f9ff5b82: (403)\nReason: Forbidden\nHTTP response headers: HTTPHeaderDict({'Audit-Id': 'd26aec38-ff78-4b85-8056-f6bd283a5069', 'Cache-Control': 'no-cache, private', 'Content-Type': 'application/json', 'X-Kubernetes-Pf-Flowschema-Uid': '335a665b-16c0-4bb9-8397-c926891c3c26', 'X-Kubernetes-Pf-Prioritylevel-Uid': 'cfcd0e74-dc4f-41ef-9bc9-fffa22b4d7bf', 'Date': 'Wed, 12 Mar 2025 13:24:38 GMT', 'Content-Length': '337'})\nHTTP response body: {\"kind\":\"Status\",\"apiVersion\":\"v1\",\"metadata\":{},\"status\":\"Failure\",\"message\":\"jobs.batch \\\"job-f9ff5b82\\\" is forbidden: exceeded quota: compute-quota, requested: count/jobs.batch=1, used: count/jobs.batch=10, limited: count/jobs.batch=10\",\"reason\":\"Forbidden\",\"details\":{\"name\":\"job-f9ff5b82\",\"group\":\"batch\",\"kind\":\"jobs\"},\"code\":403}\n\n",
    1
  ],
  "status_url": "/ws/job-status/('Failed to create or run job job-f9ff5b82: (403)\\nReason: Forbidden\\nHTTP response headers: HTTPHeaderDict({\\'Audit-Id\\': \\'d26aec38-ff78-4b85-8056-f6bd283a5069\\', \\'Cache-Control\\': \\'no-cache, private\\', \\'Content-Type\\': \\'application/json\\', \\'X-Kubernetes-Pf-Flowschema-Uid\\': \\'335a665b-16c0-4bb9-8397-c926891c3c26\\', \\'X-Kubernetes-Pf-Prioritylevel-Uid\\': \\'cfcd0e74-dc4f-41ef-9bc9-fffa22b4d7bf\\', \\'Date\\': \\'Wed, 12 Mar 2025 13:24:38 GMT\\', \\'Content-Length\\': \\'337\\'})\\nHTTP response body: {\"kind\":\"Status\",\"apiVersion\":\"v1\",\"metadata\":{},\"status\":\"Failure\",\"message\":\"jobs.batch \\\\\"job-f9ff5b82\\\\\" is forbidden: exceeded quota: compute-quota, requested: count/jobs.batch=1, used: count/jobs.batch=10, limited: count/jobs.batch=10\",\"reason\":\"Forbidden\",\"details\":{\"name\":\"job-f9ff5b82\",\"group\":\"batch\",\"kind\":\"jobs\"},\"code\":403}\\n\\n', 1)"
}