import requests
from datetime import datetime, timedelta
from pathlib import Path
import zipfile
import os
from clint.textui import progress
import sys


def download_gtfs_feed(directory):
    current_datetime = datetime.now()
    current_date_time = current_datetime.strftime("%Y%m%d_%H-%M-%S")

    try:
        LOG.info("Downloading started")
        url = "https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/all"

        req = requests.get(url, stream=True)

        filepath = directory / f"GTFS_{current_date_time}.zip"

        with open(filepath, "wb") as output_file:
            total_length = int(req.headers.get("content-length"))
            for chunk in progress.bar(
                req.iter_content(chunk_size=1024),
                expected_size=(total_length / 1024) + 1,
            ):
                if chunk:
                    output_file.write(chunk)
        LOG.info("Downloading Completed")

        # Extract the downloaded zip file to a folder with the same name
        extraction_folder = (
            directory / filename.stem
        )  # Using filename.stem to get the name without the extension
        extraction_folder.mkdir(exist_ok=True)

        with zipfile.ZipFile(filepath, "r") as zip_ref:
            zip_ref.extractall(extraction_folder)

        LOG.info("Extracted contents to %s", extraction_folder)

        # Create an "ss" folder and move previous files
        ss_folder = directory / "ss"
        ss_folder.mkdir(exist_ok=True)

        for file in directory.glob("GTFS_*"):
            if file != extraction_folder:
                new_location = ss_folder / file.name
                file.rename(new_location)
                LOG.info("Moved %s to %s", file.name, new_location)

    except requests.exceptions.RequestException:
        LOG.error("Download failed:", exc_info=True)

    except zipfile.BadZipFile:
        LOG.error("Failed to unzip the downloaded file.", exc_info=True)

    except Exception:
        LOG.error("An error occurred:", exc_info=True)


if __name__ == "__main__":
    GTFS_directory = Path("C:/Users/UKJMH006/Documents/TfN/Stage-2/GTFs_feeds/")
    folder_name = "GTFS"

    # Check for a command-line argument to force download
    if len(sys.argv) > 1 and sys.argv[1] == "--force":
        download_gtfs_feed(GTFS_directory)
    else:
        current_datetime = datetime.now()
        current_date_time = current_datetime.strftime("%Y%m%d_%H-%M-%S")

        # Get a list of folders containing the specified folder_name
        found_folders = [
            folder for folder in os.listdir(GTFS_directory) if folder_name in folder
        ]

        # Initialize variables to store the latest date and folder name
        latest_date = None
        latest_folder = None

        # Find the latest date among the folders
        for folder in found_folders:
            parts = folder.split("_")
            if len(parts) > 1:
                date = parts[1]
                try:
                    date_1 = datetime.strptime(date, "%Y%m%d")
                    if latest_date is None or date_1 >= latest_date:
                        latest_date = date_1
                        latest_folder = folder
                except ValueError:
                    print(f"Invalid date format in folder '{folder}': {date}")

        # Check if it's more than a week old
        if latest_date and (current_datetime - latest_date) >= timedelta(weeks=1):
            print(
                f"'{latest_date}' is more than a week old, attempting to download new feed."
            )
            download_gtfs_feed(GTFS_directory)

        else:
            print(
                f"No need to download. Latest folder '{latest_folder}' is less than a week old."
            )
