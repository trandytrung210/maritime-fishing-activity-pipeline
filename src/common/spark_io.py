from __future__ import annotations

import os
import shutil
import tempfile
import uuid
from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import DataFrame

# Always stage under a real Linux filesystem inside the container.
# Docker Desktop Windows bind mounts reject Hadoop chmod.
_SPARK_STAGE_ROOT = Path("/tmp/maritime_spark_write")


def write_parquet(
    df: DataFrame,
    output_path: Path | str,
    *,
    mode: str = "overwrite",
    partition_by: Sequence[str] | None = None,
) -> None:
    """Write Spark parquet via /tmp staging, then copy onto the project mount."""
    output_path = Path(output_path).resolve()
    _SPARK_STAGE_ROOT.mkdir(parents=True, exist_ok=True)

    staging_root = Path(
        tempfile.mkdtemp(
            prefix=f"job_{os.getpid()}_{uuid.uuid4().hex[:8]}_",
            dir=str(_SPARK_STAGE_ROOT),
        )
    )
    staging_data = staging_root / "data"

    if not str(staging_data).startswith("/tmp/"):
        raise RuntimeError(
            f"Refusing to stage Spark write outside /tmp: {staging_data}"
        )

    print(f"Spark parquet staging: {staging_data}")
    print(f"Spark parquet final:   {output_path}")

    try:
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        # file:// URI avoids ambiguous relative-path resolution by Hadoop
        writer.parquet(staging_data.resolve().as_uri())

        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            shutil.rmtree(output_path)

        _copy_parquet_tree(staging_data, output_path)
        print(f"Parquet write successful: {output_path}")
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)


def _copy_parquet_tree(src: Path, dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for item in src.rglob("*"):
        if item.name.endswith(".crc"):
            continue
        relative = item.relative_to(src)
        target = dst / relative
        if item.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        elif item.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(item, target)


def write_parquet_replace_partitions(
    df: DataFrame,
    output_root: Path | str,
    *,
    partition_by: Sequence[str] = ("year", "month"),
    mode: str = "overwrite",
) -> list[Path]:
    """Write partitioned parquet and replace only produced partition folders."""
    output_root = Path(output_root).resolve()
    _SPARK_STAGE_ROOT.mkdir(parents=True, exist_ok=True)
    staging_root = Path(
        tempfile.mkdtemp(
            prefix=f"job_{os.getpid()}_{uuid.uuid4().hex[:8]}_",
            dir=str(_SPARK_STAGE_ROOT),
        )
    )
    staging_data = staging_root / "data"
    replaced: list[Path] = []

    print(f"Spark parquet staging: {staging_data}")
    print(f"Spark parquet root:    {output_root}")

    try:
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(staging_data.resolve().as_uri())

        output_root.mkdir(parents=True, exist_ok=True)
        # Partition folders look like year=2020/month=01
        partition_dirs = [
            path
            for path in staging_data.rglob("*")
            if path.is_dir()
            and path.name.startswith(f"{partition_by[-1]}=")
        ]
        if not partition_dirs:
            # Fallback: treat staging_data itself as the dataset root
            if output_root.exists():
                shutil.rmtree(output_root)
            _copy_parquet_tree(staging_data, output_root)
            replaced.append(output_root)
            return replaced

        for part_dir in partition_dirs:
            relative = part_dir.relative_to(staging_data)
            target = output_root / relative
            if target.exists():
                shutil.rmtree(target)
            target.parent.mkdir(parents=True, exist_ok=True)
            _copy_parquet_tree(part_dir, target)
            replaced.append(target)
            print(f"Replaced partition: {target}")

        # Keep a root _SUCCESS marker for readers that expect it
        (output_root / "_SUCCESS").write_text("", encoding="utf-8")
        return replaced
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)
