# Databricks notebook source
# Github Ingestion

import requests
import os

dbutils.widgets.text("catalog", "adb_real_estate_mb")
CATALOG = dbutils.widgets.get("catalog")

REPO = "NEELKALYANI/Chennai-Databricks-Project-Data"
BRANCH = "main"
FOLDER = "Chennai"
VOLUME_PATH = f"/Volumes/{CATALOG}/raw/github_chennai/listings/"

def download_github_folder(repo, branch, folder, dest):
    api_url = f"https://api.github.com/repos/{repo}/contents/{folder}?ref={branch}"
    response = requests.get(api_url)
    
    if response.status_code != 200:
        raise Exception(f"GitHub API error: {response.status_code} - {response.text}")
    
    files = response.json()
    downloaded = []
    
    for f in files:
        if f["type"] == "file":
            print(f"Downloading: {f['name']} ({f['size']} bytes)...")
            content = requests.get(f["download_url"]).content
            out_path = os.path.join(dest, f["name"])
            with open(out_path, "wb") as out:
                out.write(content)
            downloaded.append(f["name"])
            print(f"  ✓ Saved to {out_path}")
    
    print(f"\nDone. {len(downloaded)} file(s) downloaded to {dest}")
    return downloaded

download_github_folder(REPO, BRANCH, FOLDER, VOLUME_PATH)

# COMMAND ----------

# Google Drive Ingestion

import requests
import os

FOLDER_ID = "109_xILvfOyl4Iusf1JPbgBdwsUDCRT9A"
VOLUME_PATH = f"/Volumes/{CATALOG}/raw/google_drive_mumbai/listings/"
API_KEY = dbutils.secrets.get(scope="mb_real_estate", key="gdrive_api_key")

def list_gdrive_files(folder_id, api_key):
    url = "https://www.googleapis.com/drive/v3/files"
    params = {
        "q": f"'{folder_id}' in parents",
        "fields": "files(id, name, size)",
        "key": api_key,
        "pageSize": 100
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"Drive API error: {response.status_code} - {response.text}")
    return response.json().get("files", [])

def download_gdrive_file(file_id, file_name, dest, api_key):
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}"
    params = {"alt": "media", "key": api_key}
    response = requests.get(url, params=params, stream=True)
    out_path = os.path.join(dest, file_name)
    with open(out_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return out_path

files = list_gdrive_files(FOLDER_ID, API_KEY)
print(f"Found {len(files)} files in Google Drive folder")

for file in files:
    print(f"Downloading: {file['name']}...")
    path = download_gdrive_file(file["id"], file["name"], VOLUME_PATH, API_KEY)
    print(f"  ✓ Saved to {path}")

print("\nAll done!")
display(dbutils.fs.ls(VOLUME_PATH))

# COMMAND ----------

# Azure Blob (ADLS Gen2) source Ingestion
SOURCE = "abfss://magicbricks-bangalore@neeladlsrealestate.dfs.core.windows.net/"

# Databricks Volume destination
DEST = f"/Volumes/{CATALOG}/raw/azure_blob_bangalore/listings/"

files = dbutils.fs.ls(SOURCE)

print(f"Found {len(files)} files in Azure Blob.\n")

display(files)

files = dbutils.fs.ls(SOURCE)

copied = 0

for file in files:
    if file.name.endswith(".json"):
        dbutils.fs.cp(
            file.path,
            DEST + file.name
        )
        copied += 1
        print(f"Copied: {file.name}")

print(f"\nSuccessfully copied {copied} JSON files.")