
-- dbt_project/models/marts/dim_pokemon.sql
--
-- PURPOSE: The canonical Pokémon entity for BI queries.
-- DESIGN: Wide table — denormalise types and abilities into arrays for ease of use.
--         BI tools struggle with multi-row-per-entity models.
--
-- MATERIALISATION: Table (not view).
--   Marts are queried heavily by BI tools. We materialise as a table so
--   each BI query doesn't re-execute the staging joins.

{{
  config(
    materialized = 'incremental',
    unique_key   = 'pokemon_id',
    on_schema_change = 'append_new_columns'
  )
}}
-- WHY INCREMENTAL?
-- New Pokémon are added by dlt with higher IDs. Using incremental materialisation
-- means dbt only processes rows that were added since the last dbt run.
-- on_schema_change='append_new_columns' handles dlt schema drift gracefully:
-- if dlt adds a new column to the raw table, dbt adds it to the mart without failing.

WITH pokemon AS (
    SELECT * FROM {{ ref('stg_pokemon') }}

    -- Incremental filter: only process rows newer than the last dbt run.
    -- dbt sets the 'is_incremental()' flag and manages the filter automatically.
    {% if is_incremental() %}
    WHERE dbt_updated_at > (SELECT MAX(dbt_updated_at) FROM {{ this }})
    {% endif %}
),

-- Aggregate types into a comma-separated string and array for BI flexibility
types_agg AS (
    SELECT
        pokemon_id,
        STRING_AGG(type_name, ', ' ORDER BY type_slot) AS types_combined,  -- "grass, poison"
        MAX(CASE WHEN type_slot = 1 THEN type_name END) AS primary_type,
        MAX(CASE WHEN type_slot = 2 THEN type_name END) AS secondary_type
    FROM {{ ref('stg_pokemon_types') }}
    GROUP BY pokemon_id
),

-- Aggregate abilities
abilities_agg AS (
    SELECT
        pokemon_id,
        STRING_AGG(
            ability_name, ', '
            ORDER BY ability_slot
        ) AS abilities_combined,
        COUNT(*) AS ability_count
    FROM {{ ref('stg_pokemon_abilities') }}
    GROUP BY pokemon_id
)

SELECT
    p.pokemon_id,
    p.pokemon_name,
    p.pokedex_order,
    p.is_default,
    p.height_m,
    p.weight_kg,
    p.base_experience,

    -- Types (denormalised for BI convenience)
    t.primary_type,
    t.secondary_type,
    t.types_combined,

    -- Abilities (denormalised)
    a.abilities_combined,
    a.ability_count,

    -- Audit
    p.dlt_load_id,
    p.dbt_updated_at

FROM pokemon p
LEFT JOIN types_agg     t ON p.pokemon_id = t.pokemon_id
LEFT JOIN abilities_agg a ON p.pokemon_id = a.pokemon_id