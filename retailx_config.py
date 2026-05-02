"""
Shared configuration for the RetailX Smart Retail Analytics Platform.

Security and quality notes:
- No secrets, tokens, or connection strings are stored in code.
- All paths are resolved under an approved project directory to avoid path traversal.
- Spark options are centralized so execution is reproducible.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output"
DEFAULT_STREAM_DIR = PROJECT_ROOT / "stream_input"
DEFAULT_CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints"


@dataclass(frozen=True)
class RetailXPathConfig:
    """Validated filesystem paths used by the project."""

    data_dir: Path = DEFAULT_DATA_DIR
    output_dir: Path = DEFAULT_OUTPUT_DIR
    stream_dir: Path = DEFAULT_STREAM_DIR
    checkpoint_dir: Path = DEFAULT_CHECKPOINT_DIR

    def ensure_directories(self) -> None:
        """Create required local directories."""
        for directory in (
            self.data_dir,
            self.output_dir,
            self.stream_dir,
            self.checkpoint_dir,
        ):
            ensure_safe_child_path(directory).mkdir(parents=True, exist_ok=True)


def ensure_safe_child_path(path: Path) -> Path:
    """
    Resolve a path and ensure it remains inside this project workspace.

    This prevents accidental reads/writes outside the repository when paths are
    supplied through command-line arguments or notebooks.
    """

    resolved = path.resolve()
    if PROJECT_ROOT not in resolved.parents and resolved != PROJECT_ROOT:
        raise ValueError(f"Unsafe path outside project workspace: {resolved}")
    return resolved


def default_spark_builder(app_name: str):
    """Return a SparkSession builder with conservative local defaults."""

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.sql.shuffle.partitions", "4")
    )

