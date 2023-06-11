import os
import sys
import tempfile
import hashlib
import boto3

endpoint_url = 'http://172.17.0.2:9000'
key_id = 'minio'
access_key = 'miniokey'

s3_client = boto3.client('s3',
                         endpoint_url=endpoint_url,
                         aws_access_key_id=key_id,
                         aws_secret_access_key=access_key,
                         aws_session_token=None,
                         config=boto3.session.Config(signature_version='s3v4'),
                         verify=False,
                         )

s3_resource = boto3.resource('s3',
                             endpoint_url=endpoint_url,
                             aws_access_key_id=key_id,
                             aws_secret_access_key=access_key,
                             aws_session_token=None,
                             config=boto3.session.Config(signature_version='s3v4'),
                             verify=False,
                             )


def md5(file_name):  # we use md5 to compare file with same name
    hash_md5 = hashlib.md5()
    with open(file_name, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class BucketWrapper:
    def __init__(self, name: str):
        already_exist = False
        for existing_bucket in s3_client.list_buckets()['Buckets']:
            if name == existing_bucket["Name"]:
                already_exist = True

        if already_exist:
            self.bucket = s3_resource.Bucket(name=name)
        else:
            self.bucket = s3_resource.create_bucket(Bucket=name)

    def upload_file(self, file_path: str, object_name: str):
        print("Upload", file_path, "(Key =", object_name + ")")
        self.bucket.upload_file(Filename=file_path, Key=object_name)

    def download_file(self, object_name: str, file_path: str):
        self.bucket.download_file(object_name, file_path)

    def remove_objects(self, objects_name_list: list[str]):
        files_to_delete = []
        for object_name in objects_name_list:
            files_to_delete.append({'Key': object_name})
            print("Remove file with key =", object_name)

        if files_to_delete:
            self.bucket.delete_objects(Delete={'Objects': files_to_delete})

    def get_objects_name_list(self):
        object_list = []
        for bucket_object in self.bucket.objects.all():
            object_list.append(bucket_object.key)
        return object_list


class SynchronizeDirectory:
    def __init__(self, directory_path: str, bucket_name: str):
        self.bucket = BucketWrapper(bucket_name)
        self.directory_path = directory_path
        self.files_to_remove = self.bucket.get_objects_name_list()
        self.tmpFilePath = tempfile.NamedTemporaryFile().name

    def synchronize(self):
        self.__synchronize_directory(self.directory_path)
        self.bucket.remove_objects(self.files_to_remove)

    def __synchronize_directory(self, directory_path: str):
        for relative_path in os.listdir(directory_path):
            path = directory_path + '/' + relative_path
            if os.path.isfile(path):
                self.__synchronize_file(path)
            else:
                self.__synchronize_directory(path)
        return

    def __synchronize_file(self, file_path: str):
        file_object_name = os.path.relpath(file_path, self.directory_path)
        if file_object_name in self.files_to_remove:
            self.files_to_remove.remove(file_object_name)  # file is in the directory, so we don't need to remove him !
            self.bucket.download_file(file_object_name, self.tmpFilePath)  # copy in tmp file to compare !
            if md5(self.tmpFilePath) == md5(file_path):
                return  # It's same file, don't need to push this file

        self.bucket.upload_file(file_path, file_object_name)

    def check_synchronization(self):
        print("Start to check synchronization...")
        objects_list = self.bucket.get_objects_name_list()
        directory_is_synchonize = self.__check_directory(self.directory_path, objects_list)
        if not directory_is_synchonize:
            print("Directory is not synchronized")
            return

        # All local file are on the bucket, we just need to check if there is not another file on the bucket
        count_local_files = 0
        for root_dir, cur_dir, files in os.walk(self.directory_path):
            count_local_files += len(files)

        if len(objects_list) != count_local_files :
            print("Bucket contains too many objects")
            print("Directory is not synchronized")
            return

        print("Directory is synchronize !")

    def __check_directory(self, directory_path: str, existing_objects: list[str]):
        all_good = True
        for relative_path in os.listdir(directory_path):
            path = directory_path + '/' + relative_path
            if os.path.isfile(path):
                all_good &= self.__check_file(path, existing_objects)
            else:
                all_good &= self.__check_directory(path, existing_objects)
        return all_good

    def __check_file(self, file_path: str, existing_objects: list[str]):
        file_object_name = os.path.relpath(file_path, self.directory_path)
        if file_object_name not in existing_objects:
            print("This file is missing on the bucket :: ", file_object_name)
            return False

        self.bucket.download_file(file_object_name, self.tmpFilePath)  # copy in tmp file to compare !
        if md5(self.tmpFilePath) != md5(file_path):
            print("This file is not the same on the bucket :: ", file_object_name)
            return False

        return True

def main():
    directory_path = sys.argv[1]
    if not os.path.exists(directory_path):
        print("ERROR: Path doesn't exist")
        return
    if not os.path.isdir(directory_path):
        print("ERROR: It's not directory path")
    bucket_name = sys.argv[2]

    synchro = SynchronizeDirectory(directory_path, bucket_name)
    synchro.synchronize()
    print("Finish to synchronize !")
    #synchro.check_synchronization()

if __name__ == "__main__":
    main()
