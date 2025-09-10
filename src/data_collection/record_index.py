"""Record index management for Wikipedia network data collection.

This module provides the RecordIndex class for tracking visited and failed articles
to prevent duplicate processing and maintain state between batch runs.
"""

import logging
import os
import shutil
from dataclasses import dataclass, field


@dataclass
class RecordIndex:
    """Track visited and failed articles to prevent duplicate processing.

    This class maintains records in text files between batches and provides
    methods for adding new records and managing state persistence.
    """

    batch_folder: str
    previous_batch_folder: str | None
    logger: logging.Logger
    file_name: str = "visited_articles.txt"
    records: set[str] = field(init=False)
    number_of_records: int = field(default=0, init=False)
    record_index_file: str = field(init=False)
    number_of_files: int = field(default=1, init=False)
    is_batch_finished: bool = field(default=False, init=False)

    def __post_init__(self):
        """Initialize the record index by setting up files and loading existing records."""
        self.record_index_file = os.path.join(self.batch_folder, self.file_name)
        if self.previous_batch_folder is not None:
            self._initialise_from_previous_batch()
        else:
            open(self.record_index_file, "w", encoding="utf-8").close()
            self.logger.info(
                "No previous batch available. Created a new empty record index in current batch folder under the name {self.file_name}"
            )
        self.records = self._load_record_index_from_file()

    def _initialise_from_previous_batch(self) -> None:
        if self.previous_batch_folder is None:
            raise ValueError("No previous batch folder provided.")
        source_file = os.path.join(self.previous_batch_folder, self.file_name)
        if os.path.exists(source_file):
            shutil.copy(source_file, self.record_index_file)
            self.logger.info(
                "Found record index file for previous batch and copied to current batch folder under the name {self.file_name}"
            )
        else:
            open(self.record_index_file, "w", encoding="utf-8").close()
            self.logger.info(
                "No record index file found for previous batch. Created a new empty record index in current batch folder under the name {self.file_name}"
            )
        return None

    def _load_record_index_from_file(self) -> set[str]:
        with open(self.record_index_file, "r", encoding="utf-8") as file:
            visited_articles = set(file.read().splitlines())
        self.logger.info("Existing record index loaded from {self.file_name}")
        return visited_articles

    # Methods for modifying the record index
    def add_record(self, record_to_add: str, write_to_file: bool = False) -> None:
        """Add a single record to the index."""
        self.records.add(record_to_add)
        if write_to_file:
            self._save_specified_records_to_file({record_to_add})
        return None

    def add_multiple_records(self, records_to_add: set[str], write_to_file: bool = False) -> None:
        """Add multiple records to the index."""
        self.records.update(records_to_add)
        if write_to_file:
            self._save_specified_records_to_file(records_to_add)
        return None

    # Methods for saving the record index
    def _save_specified_records_to_file(self, articles: set[str]) -> None:
        with open(self.record_index_file, "a", encoding="utf-8") as file:
            for article in articles:
                file.write(article + "\n")
        self.logger.info("Visited articles saved to file")
        return None

    def _save_all_records_to_file(self) -> None:
        if not self.is_batch_finished:
            self.logger.warning(
                "This method is computationally expensive and should only be used at the end of a batch"
            )

        self.number_of_files += 1
        back_up_file_name = self.file_name.split(".", maxsplit=1)[0] + f"_{self.number_of_files}.txt"
        back_up_record_index_file = os.path.join(self.batch_folder, back_up_file_name)
        shutil.move(self.record_index_file, back_up_record_index_file)
        self.logger.debug("Previous record index file backed up as '%s'", back_up_file_name)

        with open(self.record_index_file, "w", encoding="utf-8") as new_file:
            for r in self.records:
                new_file.write(r + "\n")
        self.logger.info("Saved all records in index to '{self.reconrd_index_file}'")
        return None

    def finish_batch(self) -> None:
        """Mark the batch as finished and save all records to file."""
        self.is_batch_finished = True
        self._save_all_records_to_file()
        return None
