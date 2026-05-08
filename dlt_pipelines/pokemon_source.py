"""
dlt Source Definition for PokéAPI.

ARCHITECTURE DECISIONS:
- @dlt.source groups resources under one namespace, sharing state and schema.
- pokemon_index uses write_disposition="replace" — it's a small lookup table with
  no reliable cursor. Full refresh is correct and safe here.
- pokemon_detail uses write_disposition="merge" + primary_key="id" for idempotency.
  Re-running the pipeline will UPDATE existing rows rather than INSERT duplicates.
- dlt.sources.incremental tracks a cursor (pokemon ID) in the pipeline's state store,
  which lives inside DuckDB itself (_dlt_pipeline_state table). On first run, it
  loads everything from id=0. On subsequent runs, it only loads new IDs.
- Nested JSON (abilities, types, stats) is yielded as raw Python lists/dicts.
  dlt's normalizer automatically creates child tables with correct foreign keys.
  You write ZERO flattening code.
"""


import dlt
from dlt.sources.helpers import requests   # dlt's requests wrapper adds retry logic
from typing import Iterator, Optional

# --- Constants ---
BASE_URL      = "https://pokeapi.co/api/v2"
DEFAULT_LIMIT = 100    # PokéAPI max per page


def _get_current_load_id() -> Optional[str]:
    """Return active dlt load_id when available, else None (eg unit tests)."""
    try:
        return (dlt.current.load_package_state() or {}).get("load_id")
    except Exception:
        # Outside an active dlt load context (eg plain unit tests), there is no
        # load package state injected into the container.
        return None


@dlt.source(name="pokemon_api")
def pokemon_source(
    limit_per_page: int = DEFAULT_LIMIT,
    max_pokemon:    int = 300,
) -> list:
    """
    Top-level dlt source. Groups all Pokémon resources.

    WHY return a list?
    Returning multiple resources from a @dlt.source means they are loaded
    in a single 'load package'. This is atomic: either all tables succeed
    or the load is rolled back. It also means they share one entry in
    _dlt_loads, making audit easier.

    Parameters:
        limit_per_page: Items per API page request.
        max_pokemon:    Safety cap for dev. Set to a large number (or None) in prod.
    """
    return [
        pokemon_index_resource(limit_per_page=limit_per_page, max_pokemon=max_pokemon),
        pokemon_detail_resource(limit_per_page=limit_per_page, max_pokemon=max_pokemon),
    ]


@dlt.resource(
    name="pokemon_index",
    write_disposition="replace",   # Full refresh every run
    primary_key="name",
)
def pokemon_index_resource(
    limit_per_page: int = DEFAULT_LIMIT,
    max_pokemon:    int = 300,
) -> Iterator[dict]:
    """
    Loads the Pokémon name/URL index.

    WHY 'replace' here?
    This table has ~1000 rows. It has no updated_at field, and the
    PokéAPI doesn't provide one. Using 'merge' without a cursor would
    require fetching all 1000 rows anyway to check for changes.
    'replace' is semantically correct and operationally simpler for
    reference/lookup tables where full reload is cheap.
    """
    offset       = 0
    total_loaded = 0

    while total_loaded < max_pokemon:
        batch_size = min(limit_per_page, max_pokemon - total_loaded)

        response = requests.get(
            f"{BASE_URL}/pokemon",
            params={"limit": batch_size, "offset": offset},
            timeout=30,
        )
        response.raise_for_status()   # Raises on 4xx/5xx — dlt will catch and retry
        data    = response.json()
        results = data.get("results", [])

        if not results:
            break

        # Yielding a list — dlt normalises each dict individually
        yield results

        total_loaded += len(results)
        offset       += len(results)

        if total_loaded >= data.get("count", max_pokemon):
            break


@dlt.resource(
    name="pokemon_detail",
    write_disposition="merge",     # Idempotent: re-run = UPDATE not INSERT
    primary_key="id",              # dlt uses this for merge matching
)
def pokemon_detail_resource(
    # dlt.sources.incremental is the core mechanism for stateful loading.
    # It tracks the maximum seen value of the cursor field ("id") across runs.
    # The state is stored in DuckDB: SELECT * FROM _dlt_pipeline_state;
    # On first run: initial_value=0, so all records are loaded.
    # On subsequent runs: only records with id > last_max_id are fetched.
    pokemon_ids: dlt.sources.incremental[int] = dlt.sources.incremental(
        "id",           # The field name in your yielded dicts to use as cursor
        initial_value=0,
    ),
    limit_per_page: int = DEFAULT_LIMIT,
    max_pokemon:    int = 300,
) -> Iterator[dict]:
    """
    Loads full Pokémon detail records, including nested structures.

    NESTED JSON BEHAVIOUR:
    When you yield a dict containing a list-of-dicts (e.g., 'abilities'),
    dlt creates a child table named: pokemon_detail__abilities
    That child table contains:
      - All fields from the nested dict
      - _dlt_parent_id: FK to the parent row's _dlt_id
      - _dlt_list_idx: Position in the original array (for ordering)
    You never write a JOIN to reconstruct this — dbt handles it in staging.

    AUDIT COLUMNS:
    Always add _loaded_at. This lets your dbt models know exactly which
    pipeline run produced each row. The load_id is the key into _dlt_loads.
    """
    offset       = 0
    total_loaded = 0
    last_seen_id = pokemon_ids.last_value   # dlt's managed cursor value

    while total_loaded < max_pokemon:
        batch_size = min(limit_per_page, max_pokemon - total_loaded)

        response = requests.get(
            f"{BASE_URL}/pokemon",
            params={"limit": batch_size, "offset": offset},
            timeout=30,
        )
        response.raise_for_status()
        data    = response.json()
        results = data.get("results", [])

        if not results:
            break

        for entry in results:
            # Fetch the detail page for each Pokémon
            detail_resp = requests.get(entry["url"], timeout=30)
            detail_resp.raise_for_status()
            detail = detail_resp.json()

            pokemon_id = detail["id"]

            # Skip records we already have from previous runs
            if pokemon_id <= last_seen_id:
                continue

            # Yield one record at a time.
            # dlt buffers these and bulk-inserts in batches for performance.
            yield {
                # --- Identity ---
                "id":              detail["id"],
                "name":            detail["name"],
                "order":           detail.get("order"),
                "is_default":      detail.get("is_default"),

                # --- Physical attributes ---
                # PokéAPI returns height in decimetres and weight in hectograms.
                # We keep raw values here. Unit conversion happens in dbt staging.
                "height_dm":       detail.get("height"),
                "weight_hg":       detail.get("weight"),
                "base_experience": detail.get("base_experience"),

                # --- Nested structures (become child tables automatically) ---
                # Each of these becomes pokemon_detail__<key> in DuckDB
                "abilities": detail.get("abilities", []),
                "types":     detail.get("types", []),
                "stats":     detail.get("stats", []),
                # sprites is a dict-of-dicts; dlt flattens it with __ separators
                "sprites":   detail.get("sprites", {}),

                # --- Audit ---
                # dlt.current.load_package_state() returns metadata about the current load.
                # This column lets you JOIN pokemon_detail with _dlt_loads later.
                "_loaded_at": _get_current_load_id(),
            }

            total_loaded += 1
            if total_loaded >= max_pokemon:
                break

        offset += len(results)

        if total_loaded >= data.get("count", max_pokemon):
            break