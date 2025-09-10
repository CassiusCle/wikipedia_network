"""Batch chunk management for Wikipedia network data collection.

This module provides the BatchChunk class for managing individual result files
within a batch to prevent memory issues and organize large datasets.
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TextIO


@dataclass
class BatchChunk:
    """Manage individual result files within a batch to prevent memory issues.

    This class handles file size limiting, timing tracking, and status management
    for chunks of Wikipedia article data within a batch processing operation.
    """

    batch_number: int
    chunk_number: int
    batch_folder: str
    logger: logging.Logger
    results_file: str = field(init=False)
    number_of_lines: int = field(default=0, init=False)
    time_start: datetime | None = field(default=None, init=False)
    time_end: datetime | None = field(default=None, init=False)
    time_elapsed: timedelta | None = field(default=None, init=False)
    is_finished: bool = field(default=False, init=False)

    # Class constants
    MAX_LINES_RESULTS_FILE = 100_000

    def __post_init__(self) -> None:
        """Initialize the batch chunk by creating the results file."""
        # Create a new results file
        filename = f"batch_{self.batch_number}_scraping_results_{self.chunk_number}"
        self.results_file = os.path.join(self.batch_folder, f"{filename}.jsonl")
        open(self.results_file, "w", encoding="utf-8").close()

        self.logger.debug("New results file created at: '%s'", self.results_file)

        self.logger.info("Chunk {self.chunk_number} of batch {self.batch_number} initialised")

        return None

    def write_line_to_results_file(self, line: str, file: TextIO) -> bool:
        """Write a line to the results file if under the line limit."""
        if self.number_of_lines >= BatchChunk.MAX_LINES_RESULTS_FILE:
            return False

        file.write(line + "\n")
        self.number_of_lines += 1
        return True

    def finish_chunk(self) -> bool:
        """Mark the chunk as finished and calculate elapsed time."""
        if self.is_finished:
            self.logger.warning("Chunk {self.chunk_number} of batch {self.batch_number} is already finished")
            return False
        if self.time_start is None:
            self.logger.error("Chunk {self.chunk_number} of batch {self.batch_number} has not been started")
            return False

        self.time_end = datetime.now()
        self.is_finished = True
        self.time_elapsed = self.time_end - self.time_start
        self.logger.info("Chunk {self.chunk_number} of batch {self.batch_number} finished")
        return True

    def __repr__(self) -> str:
        """Return a detailed string representation of the BatchChunk."""
        return (
            f"BatchChunk(batch_number={self.batch_number!r}, "
            f"chunk_number={self.chunk_number!r}, "
            f"batch_folder={self.batch_folder!r}, "
            f"results_file={self.results_file!r}, "
            f"number_of_lines={self.number_of_lines!r}, "
            f"time_start={self.time_start!r}, "
            f"time_end={self.time_end!r}, "
            f"is_finished={self.is_finished!r})"
        )

    def __str__(self) -> str:
        """Return a human-readable string representation of the BatchChunk."""
        return (
            f"BatchChunk {self.chunk_number} of Batch {self.batch_number}:\n"
            f"  Folder: {self.batch_folder}\n"
            f"  Results File: {self.results_file}\n"
            f"  Number of Lines: {self.number_of_lines}\n"
            f"  Start Time: {self.time_start}\n"
            f"  End Time: {self.time_end}\n"
            f"  Finished: {self.is_finished}"
        )
