# new
- version changes (don't override changes.txt without keep the history)
- when a folder has been modified (only 2nd level folders), we should create a job using lidar-zip to zip it to the Lidar-zip NAS folder
- have an endpoint to get the json describing the LiDAR-Zips folder for frontend
- Add a potree converter job automatically (need more specs)

# old
- Add some persistence for the job, by using redis
- Add more tests
- Add release-please token
- Better status for job/pod monitoring
- We should probably have a backgroundTask that check the inMemory status of jobs and delete the unecessary one

- since we're having inmemory status, we chould on ping, check that the job still exist.
If not, maybe try deleting it on kube with safe failing, and then send a message and close connection

- Add an option to the start-job route to delete on job download or after 3 or 4 download (other options would be to have a background task that clean up job, and the clean up job would delete the file, not sure how to do it yet)
