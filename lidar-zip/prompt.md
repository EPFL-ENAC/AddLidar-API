# Goal
Context: I'm a fullstack devops. I'm fluent with unix/kubernetes/bash/sheel and unix systems

The archiving/compressing of the folders could be done via a job creation on kube for each folder, or in a batch.

My problem:

I have a folder with N levels of depth, I'd like to zip every 2nd level folders each time they change.

So for instance, If I have /original_root/level1/level2_a /level1/level2_b I'll end up having

/zip_root/level1/level2_a.tar.gz and  /zip_root/level1/level2_b.tar.gz

What you need to know is that /original_root is a samba mounted drive and /zip_root is another storage device

1) I need to know what could be the criterias for changed of level2 folder ? size of the content ? date of latest files changed ? recursively ?

2) I need to know what could be a solution for keeping a list of the states of the archive/compression and changes made in that folder ? could a json like this be a solution ? or is there another way?

Try to bring me a full and viable solution. Don't hesitate to be thorough

Here's the JSON:

[
  {
    "folder_path": "./lidar/0002_Val_dArpette/01_Point_Cloud",
    "folder_size_kb": 33517720,
    "folder_file_count": 0,
    "archive_path": "./lidar-zip/0002_Val_dArpette/01_Point_Cloud.tar.gz",
    "archive_size_kb": 33517,
    "archive_mod_time": "2023-05-20T14:57:39+02:00",
    "folder_mod_time": "2023-05-20T14:57:35+02:00",
    "folder_mod_time_epoch": 1745321455
  }
]
