-- dbt_project/models/marts/fct_pokemon_stats.sql
--
-- PURPOSE: One row per Pokémon per stat. Enables aggregate queries like
--          "which Pokémon has the highest attack?" or "average HP by type?"
--
-- GRAIN: pokemon_id + stat_name (e.g., bulbasaur + hp)

{{
  config(
    materialized = 'incremental',
    unique_key   = ['pokemon_id', 'stat_name'],
    on_schema_change = 'append_new_columns'
  )
}}

WITH stats AS (
    SELECT * FROM {{ ref('stg_pokemon_stats') }}
),

pokemon AS (
    SELECT pokemon_id, pokemon_name, primary_type, secondary_type
    FROM {{ ref('dim_pokemon') }}

    {% if is_incremental() %}
    WHERE dbt_updated_at > (SELECT MAX(dbt_updated_at) FROM {{ this }})
    {% endif %}
)

SELECT
    s.pokemon_id,
    p.pokemon_name,
    p.primary_type,
    p.secondary_type,
    s.stat_name,
    s.base_stat,
    s.effort_value,

    -- Rank within each stat for leaderboard queries
    RANK() OVER (
        PARTITION BY s.stat_name
        ORDER BY s.base_stat DESC
    ) AS stat_rank,

    CURRENT_TIMESTAMP AS dbt_updated_at

FROM stats s
INNER JOIN pokemon p ON s.pokemon_id = p.pokemon_id