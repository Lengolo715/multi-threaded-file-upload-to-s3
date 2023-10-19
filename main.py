import os
import re
import json
import time
import psutil
import concurrent.futures
import boto3
import threading
import dotenv

dotenv.load_dotenv("Your path location for environment variable files")
# ----------------------------------------------------------------Variables----------------------------------------------------------------#
nas_directory_unix = os.getenv("NAS_DIRECTORY_UNIX")
nas_directory_windows = os.getenv("NAS_DIRECTORY_WINDOWS")
file_name_pattern = os.getenv("FILE_NAME_PATTERN")
folder_name_pattern = os.getenv("FOLDER_NAME_PATTERN")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")
file_list_path = os.getenv("FILE_LIST_PATH")
file_paths_to_upload = []
processing_threads_no = 5
num_files_uploaded = 0
num_files_to_be_uploaded = 0
num_files_failed = 0
missing_folders = 0
total_file_size = 0
# ------------------------------------Establish Boto Connection with AWS Security Token Service (STS)-------------------------------------#
sts_client = boto3.client(
    'sts',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN")
)

# -------------------------------------------------Assume Role After Succesful Connection-------------------------------------------------#
try:
    response = sts_client.assume_role(
        RoleArn="Your role arn",
        RoleSessionName="Your session name"
    )
    temp_credentials = response['Credentials']
    print("--" * 25)
    print("Assuming role... Assumed!")
except Exception as e:
    print("Failed to assume role!\nError message: {e}.")
# ---------------------------------------------------Establish s3 boto client Connection--------------------------------------------------#
s3_client = boto3.client(
    's3',
    aws_access_key_id = temp_credentials['AccessKeyId'],
    aws_secret_access_key = temp_credentials['SecretAccessKey'],
    aws_session_token = temp_credentials['SessionToken']
)
# #------------------------------------------Function to return list of items in a directory-----------------------------------------------#
def list_files(directory):
    try:
        return [f for f in os.listdir(directory)]
    except:
        dummy = 0

# ----------------------------------------------Function to return items in a JSON File---------------------------------------------------#
def read_file_list(file_list_location):
    with open(file_list_location, 'r') as file:
        data = json.load(file)
        files_in_file_list = data['files']
    return files_in_file_list, data


# ----------------------------------------------Function to write items to a JSON File----------------------------------------------------#
def write_file_list(file_list_location, data):
    with open(file_list_location, 'w') as file:
        json.dump(data, file, indent=4)


# -----------------------------------------Function to upload items to an s3 bucket location ---------------------------------------------#
file_lock = threading.Lock()  # synchronize file access

def upload_to_s3(local_file_path):
    try:

        global num_files_uploaded
        global num_files_failed
        file_name = os.path.basename(local_file_path)
        s3_key = f"s3_directory{file_name}"

        with open(local_file_path, 'rb') as file:
            s3_client.upload_fileobj(file, s3_bucket_name, s3_key)
        with file_lock:
            files_in_file_list, data = read_file_list(file_list_path)
            if local_file_path not in files_in_file_list:
                files_in_file_list.append(local_file_path)
                write_file_list(file_list_path, data)
                num_files_uploaded += 1
    except Exception as e:
        print(e)
        num_files_failed += 1
    finally:
        # Always release the lock, even if an exception occurs
        file_lock.release()


# -----------------------------------------Function to upload items to an s3 bucket location ---------------------------------------------#
def download_from_s3(local_file_path):
    dummy = 0


# -------------------------------------------------Function to process a directory----------------------------------------------------#
def process_directory(directory):
    global num_files_to_be_uploaded
    global total_file_size
    global missing_folders
    for file in list_files(directory):
        try:
            if re.match(file_name_pattern, file):
                file_path = os.path.join(nas_directory_windows, file)
                files_in_file_list, data = read_file_list(file_list_path)
                if file not in files_in_file_list:
                    try:
                        file_paths_to_upload.append(file_path)
                        file_size = os.path.getsize(file_path)
                        num_files_to_be_uploaded += 1
                        total_file_size += file_size
                    except Exception as e:
                        print(f"Error processing file {file}:\n{e}")
        except Exception as e:
            print(f"Error message: {e}")
            pass

# -------------------------------------------------Function to print logs----------------------------------------------------#
def logs():
    global num_files_to_be_uploaded
    global total_file_size
    total_file_size = round(total_file_size / (1024 **3), 2)
    print("--" * 25)
    print(f"Files to be Uploaded: {num_files_to_be_uploaded}")
    print(f"Total file size: {total_file_size} MB")
    print("--" * 25)


# -------------------------------------------------Function to be invoked initially--------------------------------------------#
def main():
    global processing_threads_no
    process_directory(nas_directory_unix)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=processing_threads_no) as executor:
            futures = [executor.submit(upload_to_s3, file_path) for file_path in file_paths_to_upload]
        concurrent.futures.wait(futures)
        logs()
        print(f"Files Uploaded: {num_files_uploaded}")
        print(f"Files Failed: {num_files_failed}")
    except Exception as e:
        print(f"Error in main: {e}")


# -------------------------------------------------Statement to run main function----------------------------------------------#
if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    execution_time = round(end_time - start_time, 2)
    print("--" * 25)
    print(f"Execution time estimate: {execution_time} seconds")
    print('The CPU usage is: ', psutil.cpu_percent(interval=0.1), '%')
    print('RAM memory % used:', psutil.virtual_memory()[2])
    print('RAM Used (GB):', round(psutil.virtual_memory()[3] / 1000000000, 2))
    print("--" * 25)
