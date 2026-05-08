WITH source AS (
    SELECT * FROM {{ source('pokemon_raw', 'pokemon_detail__stats') }}
),

parent AS (
    SELECT _dlt_id, id AS pokemon_id
    FROM {{ source('pokemon_raw', 'pokemon_detail') }}
),

cleaned AS (
    SELECT
        p.pokemon_id,
        s.stat__name                    AS stat_name,      -- e.g., "hp", "attack", "defense"
        s.base_stat,
        s.effort                        AS effort_value    -- EVs gained by defeating this Pokémon
    FROM source s
    INNER JOIN parent p ON s._dlt_parent_id = p._dlt_id
)

SELECT * FROM cleaned