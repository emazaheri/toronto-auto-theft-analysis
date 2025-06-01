"""Utilities for ETL pipeline metrics and monitoring."""

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from src.config.logging_config import get_logger

logger = get_logger(__name__)


class ETLMetrics:
    """Class for collecting and reporting ETL process metrics.

    This class tracks various metrics during an ETL process, such as
    row counts, processing times, validation failures, and can
    save the metrics to a JSON file.
    """

    def __init__(self, pipeline_name: str, output_dir: Path | None = None):
        """Initialize the metrics collector.

        Args:
            pipeline_name: Name of the ETL pipeline
            output_dir: Directory to save metrics JSON file (default: logs directory)
        """
        self.pipeline_name = pipeline_name
        self.start_time = time.time()
        self.metrics: dict[str, Any] = {
            "pipeline_name": pipeline_name,
            "start_time": datetime.now().isoformat(),
            "row_counts": {},
            "timings": {},
            "validation": {"failures": {}, "warnings": {}},
            "memory_usage": {},
        }

        # Set up output directory
        root_dir = Path(__file__).parents[2].absolute()
        self.output_dir = output_dir if output_dir else (root_dir / "logs")
        os.makedirs(self.output_dir, exist_ok=True)

    def start_stage(self, stage_name: str) -> None:
        """Mark the start of a pipeline stage for timing.

        Args:
            stage_name: Name of the stage starting
        """
        self.metrics["timings"][f"{stage_name}_start"] = time.time()

    def end_stage(self, stage_name: str) -> float:
        """Mark the end of a pipeline stage and calculate duration.

        Args:
            stage_name: Name of the stage ending

        Returns:
            Duration of the stage in seconds
        """
        end_time = time.time()
        start_time = self.metrics["timings"].get(f"{stage_name}_start", self.start_time)
        duration = end_time - start_time
        self.metrics["timings"][f"{stage_name}_duration"] = duration
        return duration

    def record_row_count(self, stage_name: str, count: int) -> None:
        """Record the row count at a particular stage.

        Args:
            stage_name: Name of the stage
            count: Number of rows
        """
        self.metrics["row_counts"][stage_name] = count

    def record_validation_failure(self, check_name: str, count: int) -> None:
        """Record a validation failure.

        Args:
            check_name: Name of the validation check
            count: Number of failures
        """
        self.metrics["validation"]["failures"][check_name] = count

    def record_validation_warning(self, check_name: str, count: int) -> None:
        """Record a validation warning.

        Args:
            check_name: Name of the validation check
            count: Number of warnings
        """
        self.metrics["validation"]["warnings"][check_name] = count

    def record_memory_usage(self, stage_name: str, df_size_mb: float) -> None:
        """Record memory usage of a DataFrame.

        Args:
            stage_name: Name of the stage
            df_size_mb: Size of DataFrame in MB
        """
        self.metrics["memory_usage"][stage_name] = df_size_mb

    def finalize(self) -> dict[str, Any]:
        """Finalize metrics collection and calculate summary statistics.

        Returns:
            Complete metrics dictionary
        """
        end_time = time.time()
        self.metrics["end_time"] = datetime.now().isoformat()
        self.metrics["total_duration"] = end_time - self.start_time

        # Calculate additional metrics
        if (
            "extract_row_count" in self.metrics["row_counts"]
            and "final_row_count" in self.metrics["row_counts"]
        ):
            self.metrics["rows_removed"] = (
                self.metrics["row_counts"]["extract_row_count"]
                - self.metrics["row_counts"]["final_row_count"]
            )
            self.metrics["percent_removed"] = (
                self.metrics["rows_removed"]
                / self.metrics["row_counts"]["extract_row_count"]
                * 100
                if self.metrics["row_counts"]["extract_row_count"] > 0
                else 0
            )

        return self.metrics

    def save(self, suffix: str | None = None) -> str:
        """Save metrics to a JSON file.

        Args:
            suffix: Optional suffix for the filename

        Returns:
            Path to the saved metrics file
        """
        # Finalize metrics if not already done
        self.finalize()

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix_str = f"_{suffix}" if suffix else ""
        filename = f"{self.pipeline_name}_metrics_{timestamp}{suffix_str}.json"
        filepath = self.output_dir / filename

        # Write to file
        with open(filepath, "w") as f:
            json.dump(self.metrics, f, indent=2)

        logger.info(f"Saved ETL metrics to {filepath}")
        return str(filepath)

    def summary(self) -> str:
        """Generate a human-readable summary of the metrics.

        Returns:
            String summary of metrics
        """
        self.finalize()

        lines = [
            f"ETL Pipeline: {self.pipeline_name}",
            f"Run time: {self.metrics['start_time']} - {self.metrics['end_time']}",
            f"Total duration: {self.metrics['total_duration']:.2f} seconds",
            "",
            "Row counts:",
        ]

        for stage, count in self.metrics["row_counts"].items():
            lines.append(f"  {stage}: {count:,}")

        if "rows_removed" in self.metrics:
            lines.append(
                f"  Rows removed: {self.metrics['rows_removed']:,} "
                f"({self.metrics['percent_removed']:.1f}%)"
            )

        lines.append("")
        lines.append("Timings:")

        for metric, value in self.metrics["timings"].items():
            if metric.endswith("_duration"):
                stage = metric.replace("_duration", "")
                lines.append(f"  {stage}: {value:.2f} seconds")

        if (
            self.metrics["validation"]["failures"]
            or self.metrics["validation"]["warnings"]
        ):
            lines.append("")
            lines.append("Validation issues:")

            for check, count in self.metrics["validation"]["failures"].items():
                lines.append(f"  {check} failures: {count:,}")

            for check, count in self.metrics["validation"]["warnings"].items():
                lines.append(f"  {check} warnings: {count:,}")

        return "\n".join(lines)
