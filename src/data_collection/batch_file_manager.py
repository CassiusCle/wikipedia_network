import os
import re
import json
import logging
from datetime import datetime
import shutil
from dataclasses import dataclass, field

from data_collection.batch_chunk import BatchChunk
from data_collection.record_index import RecordIndex
from data_collection.batch_metadata import BatchMetadata

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger(__name__)
    if not logger.handlers:  # Check if handlers already exist
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

@dataclass
class BatchFileManager:

    data_folder: str | None
    previous_batch_folder: str | None
    logger: logging.Logger = field(default_factory=_setup_logger)

    batch_number: int = field(init=False)
    batch_folder: str = field(init=False)
    chunks: list[BatchChunk] = field(init=False)
    time_start: datetime | None = field(init=False)
    time_end: datetime | None = field(init=False)
    visited_articles: RecordIndex = field(init=False)
    failed_articles: RecordIndex = field(init=False)
    batch_metadata: BatchMetadata = field(init=False)
    


    # Instance attributes
    # data_folder: str
    # batch_folder: str
    # previous_batch_folder: str | None
    # batch_number: int
    # number_of_results_files: int
    # path_active_file: str
    # number_of_lines_active_file: int
    # chunks: list[BatchChunk]
    # batch_metadata_file: str
    # time_start: datetime

    def __post_init__(self):
        self.logger.info("BatchFileManager is initialising the current batch")

        # Set-up batch
        if self.previous_batch_folder is not None:
            self.batch_number, self.data_folder = self._set_up_from_previous_batch_folder(
                self.previous_batch_folder
            )
        else:
            self.logger.info("No previous batch folder provided.")
            self.batch_number, self.data_folder, self.previous_batch_folder = (
                self.set_up_from_data_path_or_nothing(self.data_folder)
            )

        self.batch_folder = self._create_new_batch_folder()

        # Set-up visited articles index and failed articles index
        self.visited_articles = RecordIndex(
            batch_folder=self.batch_folder,
            previous_batch_folder=self.previous_batch_folder,
            file_name="visited_articles.txt",
            logger=self.logger
        )
        
        self.failed_articles = RecordIndex(
            batch_folder=self.batch_folder,
            previous_batch_folder=self.previous_batch_folder,
            file_name="failed_articles.txt",
            logger=self.logger
        ) # These are the articles that failed to be scraped

        # Set-up data storage mechanism
        self.active_chunk = BatchChunk(
            batch_number=self.batch_number,
            chunk_number=1,
            batch_folder=self.batch_folder,
            logger=self.logger
        )
        self.chunks = [self.active_chunk]

        # Initialise metadata file
        self.batch_metadata = BatchMetadata() #self.create_metadata_file() # TODO: Replace with Metadata class

        self.logger.info(
            "BatchFileManager has finished initialising batch number %s at '%s'",
            self.batch_number,
            self.batch_folder,
        )
        return None

    # def __init__(
    #     self,
    #     previous_batch_folder: str | None = None,
    #     data_path: str | None = None,
    #     logger: logging.Logger | None = None,
    # ) -> None:

    def _set_up_from_previous_batch_folder(self, previous_batch_folder: str) -> tuple[int, str]:
        self.logger.info("Previous batch folder provided: %s", previous_batch_folder)
        match_previous_batch_number = re.search(
            r"_batch_(\d+)$", previous_batch_folder.split("/")[-1]
        )
        if match_previous_batch_number:
            batch_number = int(match_previous_batch_number.group(1)) + 1
            self.logger.info(
                "Batch number determined from previous batch folder: '%s'",
                batch_number,
            )
        else:
            self.logger.error("Invalid previous batch folder name.")
            raise ValueError("Invalid previous batch folder name.")

        # Get the data folder path
        data_folder = os.path.abspath(os.path.join(previous_batch_folder, "../.."))
        self.logger.info("Data folder path set to: '%s'", data_folder)

        return batch_number, data_folder

    def set_up_from_data_path_or_nothing(
        self, data_path: str | None
    ) -> tuple[int, str, str | None]:
        
        if data_path is None:
            data_path = self._find_or_create_data_folder()
        self.data_folder = data_path if data_path.endswith("/data") else f"{data_path}/data"
        self.logger.info("Data folder path set to: '%s'", self.data_folder)
        previous_batch_folder, previous_batch_number = self._get_recent_batch_history()
        batch_number = previous_batch_number + 1 if previous_batch_number else 1
        self.logger.info("Current batch number set to: %s", batch_number)

        return batch_number, self.data_folder, previous_batch_folder    

    def start_batch_run(self):
        self.logger.info("Starting batch run")
        self.time_start = datetime.now()

    # def _create_or_get_visited_articles_file(self) -> str:
    #     target_file = os.path.join(self.batch_folder, "visited_articles.txt")

    #     if self.previous_batch_folder:
    #         source_file = os.path.join(
    #             self.previous_batch_folder, "visited_articles.txt"
    #         )
    #         if os.path.exists(source_file):
    #             shutil.copy(source_file, target_file)
    #             self.logger.info(
    #                 "Found visited articles file for previous batch and copied to current batch folder"
    #             )
    #         else:
    #             open(target_file, "w", encoding="utf-8").close()
    #             self.logger.info(
    #                 "No visited articles file found for previous batch, thus created a blank file in current batch folder"
    #             )
    #     else:
    #         open(target_file, "w", encoding="utf-8").close()
    #         self.logger.info(
    #             "No previous batch available. Created a blank visited articles file in current batch folder"
    #         )

    #     return target_file

    def create_metadata_file(self) -> str:
        metadata_file_path = os.path.join(self.batch_folder, "metadata.jsonl")
        open(metadata_file_path, "w", encoding="utf-8").close()
        self.logger.info(f"Metadata file created at: '{metadata_file_path}'")
        return metadata_file_path

    def _find_or_create_data_folder(self) -> str:
        """Add docstring"""
        root_path = BatchFileManager._find_project_root()
        for dirpath, dirnames, _ in os.walk(root_path):
            if "data" in dirnames:
                data_folder = os.path.join(dirpath, "data")
                self.logger.info(f"Data folder found at: '{data_folder}'")
                return data_folder

        # If no data folder is found, create one
        data_folder = os.path.join(root_path, "data")
        os.makedirs(data_folder)
        self.logger.info(f"No data folder found, created one at: '{data_folder}'")

        return data_folder

    def _get_recent_batch_history(self) -> tuple[str | None, int | None]:
        """Add docstring"""

        # Find or create the "staging" subfolder in the "data" folder
        staging_folder = os.path.join(self.data_folder, "staging")
        if not os.path.exists(staging_folder):
            os.makedirs(staging_folder)

        # Get the list of subfolders in the "staging" folder
        subfolders = [f.name for f in os.scandir(staging_folder) if f.is_dir()]

        # Find the batch folders with names like "YYYYMMDD_batch_{batch number}"
        pattern = re.compile(r"^\d{8}_batch_\d+$")
        batch_folders = [f for f in subfolders if pattern.match(f)]

        # Determine the number of the next batch and the path of the previous batch
        if batch_folders:
            previous_batch_number = max(
                [int(f.split("_batch_")[1]) for f in batch_folders]
            )

            previous_batch_folder_name = [
                f
                for f in batch_folders
                if int(f.split("_batch_")[1]) == previous_batch_number
            ][0]
            previous_batch_folder = os.path.join(
                staging_folder, previous_batch_folder_name
            )
            self.logger.info(
                f"Previous batch folder found at: '{previous_batch_folder}'"
            )
        else:
            previous_batch_folder = None
            previous_batch_number = None

        return previous_batch_folder, previous_batch_number

    def _create_new_batch_folder(self) -> str:
        """Add docstring"""
        staging_folder = os.path.join(self.data_folder, "staging")

        # Create the new batch folder
        new_batch_folder_name = (
            f"{datetime.now().strftime('%Y%m%d')}_batch_{self.batch_number}"
        )
        new_batch_folder = os.path.join(staging_folder, new_batch_folder_name)
        os.makedirs(new_batch_folder)
        self.logger.info(f"New batch folder created at: '{new_batch_folder}'")

        return new_batch_folder

    @staticmethod
    def _find_project_root(max_levels=4):
        root_files = [
            "LICENSE",
            "README.md",
            "requirements.txt",
        ]

        def scan_directory(directory):
            for dirpath, _, filenames in os.walk(directory):
                if any(file in filenames for file in root_files):
                    return dirpath
            return None

        current_path = os.getcwd()
        levels_checked = 0

        while levels_checked < max_levels:
            # Scan the current directory and all subdirectories
            result = scan_directory(current_path)
            if result:
                return result

            # Move up one level
            current_path = os.path.dirname(current_path)
            levels_checked += 1

        return None
