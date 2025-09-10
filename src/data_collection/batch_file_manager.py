"""Batch file management for Wikipedia network data collection.

This module provides the BatchFileManager class for orchestrating the entire
batch processing system including folder management, metadata tracking, and
coordination of article processing operations.
"""

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime

from data_collection.batch_chunk import BatchChunk
from data_collection.batch_metadata import BatchMetadata
from data_collection.record_index import RecordIndex


def _setup_logger() -> logging.Logger:
    """Set up a logger for the BatchFileManager class."""
    logger = logging.getLogger(__name__)
    if not logger.handlers:  # Avoid adding handlers multiple times
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


@dataclass
class BatchFileManager:
    """
    Manages batch operations including folder structure, metadata, and article processing.

    Attributes:
        data_folder (str | None): The path to the data folder.
        previous_batch_folder (str | None): Path to the folder containing the previous batch.
        logger (logging.Logger): Logger instance for logging messages.
    """

    data_folder: str | None = None
    previous_batch_folder: str | None = None
    logger: logging.Logger = field(default_factory=_setup_logger)

    batch_number: int = field(init=False)
    batch_folder: str = field(init=False)
    staging_folder: str = field(init=False)
    visited_articles: RecordIndex = field(init=False)
    failed_articles: RecordIndex = field(init=False)
    active_chunk: BatchChunk = field(init=False)
    chunks: list[BatchChunk] = field(init=False)
    time_start: datetime | None = field(init=False)
    time_end: datetime | None = field(init=False)
    batch_metadata: BatchMetadata = field(init=False)

    def __post_init__(self):
        """Initialize the BatchFileManager by setting up the batch number, folders, and metadata."""
        self.logger.info("BatchFileManager is initializing the current batch")

        # Set-up batch based on the previous batch folder or data folder
        if self.previous_batch_folder is not None:
            self.batch_number, self.data_folder = self._set_up_from_previous_batch_folder(self.previous_batch_folder)
        else:
            self.logger.info("No previous batch folder provided.")
            self.batch_number, self.previous_batch_folder = self._set_up_from_data_folder_or_nothing(self.data_folder)
        self.batch_folder = self._create_new_batch_folder()

        # Create the "staging" subfolder if it doesn't exist
        self.staging_folder = os.path.join(self.data_folder, "staging")
        if not os.path.exists(self.staging_folder):
            os.makedirs(self.staging_folder)

        # Set up visited and failed articles indexes
        self.visited_articles = RecordIndex(
            batch_folder=self.batch_folder,
            previous_batch_folder=self.previous_batch_folder,
            file_name="visited_articles.txt",
            logger=self.logger,
        )

        self.failed_articles = RecordIndex(
            batch_folder=self.batch_folder,
            previous_batch_folder=self.previous_batch_folder,
            file_name="failed_articles.txt",
            logger=self.logger,
        )  # These are the articles that failed to be scraped

        # Set up active chunk for batch storage
        self.active_chunk = BatchChunk(
            batch_number=self.batch_number, chunk_number=1, batch_folder=self.batch_folder, logger=self.logger
        )
        self.chunks = [self.active_chunk]

        # Initialize metadata file
        self.batch_metadata = BatchMetadata()  # self.create_metadata_file() # TODO: Replace with Metadata class

        self.logger.info(
            "BatchFileManager has finished initialising batch number %s at '%s'",
            self.batch_number,
            self.batch_folder,
        )
        return None

    def _set_up_from_previous_batch_folder(self, previous_batch_folder: str) -> tuple[int, str]:
        """Set up the batch number and data folder based on the previous batch folder.

        Args:
            previous_batch_folder (str): The path of the previous batch folder.

        Returns:
            tuple[int, str]: The batch number and the path to the data folder.
        """
        self.logger.info("Previous batch folder provided: %s", previous_batch_folder)
        match_previous_batch_number = re.search(r"_batch_(\d+)$", previous_batch_folder.split("/")[-1])
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

    def _set_up_from_data_folder_or_nothing(self, data_folder: str | None) -> tuple[int, str | None]:
        """Set up the batch number and the previous batch folder, or create a new data folder.

        Args:
            data_folder (str | None): The path of the data folder.

        Returns:
            tuple[int, str | None]: The batch number and the previous batch folder.
        """
        if data_folder is None:
            self.data_folder = self._find_or_create_data_folder()
        self.logger.info("Data folder path set to: '%s'", self.data_folder)
        previous_batch_folder, previous_batch_number = self._get_recent_batch_history()
        batch_number = previous_batch_number + 1 if previous_batch_number else 1
        self.logger.info("Current batch number set to: %s", batch_number)

        return batch_number, previous_batch_folder

    def _find_or_create_data_folder(self) -> str:
        """Find or create a data folder in the project root.

        Returns:
            str: The path to the data folder.
        """
        root_path = BatchFileManager._find_project_root()
        for dirpath, dirnames, _ in os.walk(root_path):
            if "data" in dirnames:
                data_folder = os.path.join(dirpath, "data")
                self.logger.info(f"Data folder found at: '{data_folder}'")
                return data_folder

        # Create the data folder if not found
        data_folder = os.path.join(root_path, "data")
        os.makedirs(data_folder)
        self.logger.info(f"No data folder found, created one at: '{data_folder}'")

        return data_folder

    def _get_recent_batch_history(self) -> tuple[str | None, int | None]:
        """Retrieve the most recent batch folder and its batch number.

        Returns:
            tuple[str | None, int | None]: The path to the previous batch folder and its batch number.
        """
        subfolders = [f.name for f in os.scandir(self.staging_folder) if f.is_dir()]
        pattern = re.compile(r"^\d{8}_batch_\d+$")
        batch_folders = [f for f in subfolders if pattern.match(f)]

        if batch_folders:
            previous_batch_number = max([int(f.split("_batch_")[1]) for f in batch_folders])
            previous_batch_folder_name = [
                f for f in batch_folders if int(f.split("_batch_")[1]) == previous_batch_number
            ][0]
            previous_batch_folder = os.path.join(self.staging_folder, previous_batch_folder_name)
            self.logger.info("Previous batch folder found at: '%s'", previous_batch_folder)
        else:
            previous_batch_folder = None
            previous_batch_number = None

        return previous_batch_folder, previous_batch_number

    # Methods for managing the batch
    def start_batch_run(self):
        """Start the batch run and log the starting time."""
        self.logger.info("Starting batch run")
        self.time_start = datetime.now()

    def create_metadata_file(self):
        """Create placeholder for metadata file creation logic."""
        # TODO: Move to class, this was autogenerated
        # metadata_file_path = os.path.join(self.batch_folder, "metadata.jsonl")
        # open(metadata_file_path, "w", encoding="utf-8").close()
        # self.logger.info(f"Metadata file created at: '{metadata_file_path}'")
        # return metadata_file_path

    def _create_new_batch_folder(self) -> str:
        """Create a new batch folder based on the current date and batch number.

        Returns:
            str: The path to the new batch folder.
        """
        new_batch_folder_name = f"{datetime.now().strftime('%Y%m%d')}_batch_{self.batch_number}"
        new_batch_folder = os.path.join(self.staging_folder, new_batch_folder_name)
        os.makedirs(new_batch_folder)
        self.logger.info(f"New batch folder created at: '{new_batch_folder}'")

        return new_batch_folder

    @staticmethod
    def _find_project_root(max_levels=4):
        """Find the project root directory by searching for common root files.

        Args:
            max_levels (int): The maximum number of directory levels to search.

        Returns:
            str | None: The path to the project
        """
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
