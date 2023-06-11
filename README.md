# Description

This script is used to synchronize local directory with distant bucket (Amazon S3).

# Technologies

This script is created with :
* python version : 3.9
* boto3 (AWS SDK for python) : 1.26.151

# Syntax

```
$ python3.9 synchronizeDirectoryToBucket.py PathOfDirectory NameOfBucket
```

# Exemple setup

To test this code, you can use [Minio](https://min.io).
You can run a light server with docker :

```
$ docker run -it -e MINIO_ACCESS_KEY=minio -e MINIO_SECRET_KEY=miniokey minio/minio server /data
```

# Attention

This script only synchronize file in the specified directory (and his sub-directory), empty directory will not be push on the bucket.
You may need to change boto3 connection configuration, which can be found at the beginning of the file synchronizeDirectoryToBucket.py.
