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