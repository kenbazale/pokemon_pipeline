"""
Operational entry point for the dlt pipeline.

This file is what Airflow's PythonOperator calls.
It does three things:
  1. Configures and runs the pipeline.
  2. Captures load_info for observability.
  3. Raises an exception if anything failed (so Airflow marks the task red).

It does NOT contain source logic. That is in pokemon_source.py.
"""

import logging
import json
from datetime import datetime

import dlt
from pokemon_source import pokemon_source

logger = logging.getLogger(__name__)


def run_pipeline(
    max_pokemon: int = 300,
    destination: str = "duckdb",
    full_refresh: bool = False,
) -> dict:
    """
    Runs the Pokémon dlt pipeline.

    Args:
        max_pokemon:  Maximum Pokémon records to load (dev guard rail).
        destination:  dlt destination string ("duckdb", "bigquery", "snowflake").
        full_refresh: If True, wipes pipeline state and reloads everything.
                      Use for backfills or fixing corrupt state. Never use in
                      normal scheduled runs.

    Returns:
        A summary dict containing load metadata. Airflow pushes this to XCom
        so downstream tasks can read it for alerting and monitoring.

    Raises:
        RuntimeError: If any dlt job failed during the load.
    """

    # --- Pipeline Configuration ---
    # The pipeline object manages state, schema, and the connection to DuckDB.
    # It reads pipeline_name and dataset_name from .dlt/config.toml automatically.
    pipeline = dlt.pipeline(
        pipeline_name="pokemon_pipeline",
        destination=destination,
        dataset_name="pokemon_raw",
        full_refresh=full_refresh,
    )

    source = pokemon_source(max_pokemon=max_pokemon)

    logger.info(
        "Starting dlt pipeline run. destination=%s, full_refresh=%s, max_pokemon=%d",
        destination, full_refresh, max_pokemon
    )

    load_info = pipeline.run(source)

    # --- Build Observability Summary ---
    # load_info is a rich object. We extract what matters and return it as a
    # plain dict so it can be serialised to JSON (for XCom, logging, or a
    # monitoring table).
    load_ids = list(getattr(load_info, "loads_ids", []) or [])
    load_id = load_ids[0] if load_ids else getattr(load_info, "load_id", None)

    summary = {
        "load_id":        load_id,
        "loads_ids":      load_ids,
        "pipeline_name":  pipeline.pipeline_name,
        "dataset_name":   pipeline.dataset_name,
        "destination":    str(pipeline.destination),
        "started_at":     str(load_info.started_at),
        "finished_at":    str(load_info.finished_at),
        "schema_name":    pipeline.default_schema_name,
        "has_failed_jobs": load_info.has_failed_jobs,
        # Row counts per table: {"pokemon_detail": 150, "pokemon_index": 150, ...}
        "row_counts": {
            table: (metrics.get("inserted_rows", 0) if isinstance(metrics, dict) else 0)
            for table, metrics in (load_info.metrics or {}).items()
        },
        # Human-readable duration
        "duration_seconds": (
            load_info.finished_at - load_info.started_at
        ).total_seconds() if load_info.finished_at and load_info.started_at else None,
    }

    # --- Failure Handling ---
    # has_failed_jobs is True if ANY table failed to load.
    # We raise here so Airflow marks the task as failed and triggers retry logic.
    # Never silently swallow pipeline errors — you will not notice bad data.
    if load_info.has_failed_jobs:
        error_msg = f"dlt pipeline had failed jobs. Load ID: {summary['load_id']}"
        logger.error("%s\nFull details:\n%s", error_msg, load_info.pretty())
        raise RuntimeError(error_msg)

    logger.info(
        "Pipeline completed successfully. Load ID: %s. Row counts: %s",
        summary["load_id"],
        json.dumps(summary["row_counts"])
    )

    return summary


if __name__ == "__main__":
    """
    Direct execution for local testing:
      cd dlt_pipelines
      python pipeline_runner.py
    """
    result = run_pipeline(max_pokemon=50)   # Small batch for quick local test
    print(json.dumps(result, indent=2, default=str))