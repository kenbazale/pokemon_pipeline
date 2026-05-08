
WITH source AS (
    SELECT * FROM {{ source('pokemon_raw', 'pokemon_detail__types') }}
),

parent AS (
    SELECT _dlt_id, id AS pokemon_id
    FROM {{ source('pokemon_raw', 'pokemon_detail') }}
),

cleaned AS (
    SELECT
        s._dlt_id                       AS type_surrogate_key,
        p.pokemon_id,
        s.type__name                    AS type_name,      -- dlt: type.name → type__name
        s.slot                          AS type_slot        -- 1 = primary type, 2 = secondary
    FROM source s
    INNER JOIN parent p ON s._dlt_parent_id = p._dlt_id
)

SELECT * FROM cleaned