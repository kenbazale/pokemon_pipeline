WITH source AS (
    SELECT * FROM {{ source('pokemon_raw', 'pokemon_detail__abilities') }}
),

-- We also need the parent table to get the human-readable pokemon_id
parent AS (
    SELECT _dlt_id, id AS pokemon_id
    FROM {{ source('pokemon_raw', 'pokemon_detail') }}
),

cleaned AS (
    SELECT
        s._dlt_id                       AS ability_surrogate_key,
        p.pokemon_id,
        s.ability__name                 AS ability_name,    -- dlt flattens ability.name → ability__name
        s.is_hidden                     AS is_hidden_ability,
        s.slot                          AS ability_slot
    FROM source s
    -- Join to parent to get the real pokemon_id (not just the internal dlt key)
    INNER JOIN parent p ON s._dlt_parent_id = p._dlt_id
)

SELECT * FROM cleaned