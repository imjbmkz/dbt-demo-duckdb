import requests
import zipfile
import duckdb
from pathlib import Path

def download_zipped_data():
    # URL of the sample data to be downloaded; added a header for HTTP request
    url = "https://www.ibm.com/docs/en/SSS9AV2/attachments/Sample.Data.v3.zip"
    header = {
        "referer": "https://www.ibm.com/docs/en/scis?topic=samples-sample-csv-files",
        "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Microsoft Edge";v="128"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "Windows",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0"
    }

    # Destination of the zip folder to be downloaded 
    target_file = Path("data/sample_data.zip")

    # Delete the file if exists and create folders if unavailable
    target_file.unlink(missing_ok=True)
    target_file.parent.mkdir(parents=True, exist_ok=True)

    # Download the zipped folder via HTTP requests
    response = requests.get(url, stream=True, headers=header)
    response.raise_for_status()

    # Save the binaries into actual zip file
    with open(target_file, "wb") as fp:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            fp.write(chunk)

def extract_files():
    # Source and destination
    zipped_folder = Path("data/sample_data.zip")
    target_folder = Path("data/extracted/")

    # Create folders if unavailable
    target_folder.mkdir(parents=True, exist_ok=True)

    # Uncompress the zipped file
    with zipfile.ZipFile(zipped_folder, "r") as fz:
        fz.extractall(target_folder)

def load_csv_to_db():
    # Set the paths to source and get all CSV files
    data_folder = Path("data/extracted/Sample Data v3/")
    file_paths = list(data_folder.glob("*.csv"))
    
    # Connect to duckdb
    with duckdb.connect("dbt_demo_duckdb/dev.duckdb") as conn:

        # Loop through each CSV file and load to duckdb; ignore all errors
        for f in file_paths:
            tbl = f.name.replace(".csv", "")
            print(f"Ingesting {f} to duckdb")
            df = duckdb.read_csv(f, ignore_errors=True).to_df()
            conn.execute(f"drop table if exists {tbl}")
            conn.execute(f"create table {tbl} as select * from df")

# Run main function
if __name__=="__main__":
    download_zipped_data()
    extract_files()
    load_csv_to_db()