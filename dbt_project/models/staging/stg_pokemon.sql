WITH source AS (
    SELECT * FROM {{ source('pokemon_raw', 'pokemon_detail') }}
),

cleaned AS (
    SELECT
        -- === Identity ===
        id                              AS pokemon_id,
        name                            AS pokemon_name,
        "order"                         AS pokedex_order,
        is_default,

        -- === Physical Attributes ===
        -- PokéAPI uses decimetres (dm) and hectograms (hg).
        -- We convert to metres and kilograms here so marts don't have to.
        ROUND(height_dm  / 10.0, 2)    AS height_m,
        ROUND(weight_hg  / 10.0, 2)    AS weight_kg,
        base_experience,

        -- === Audit ===
        -- _loaded_at is the dlt load_id. Join to _dlt_loads for full run metadata.
        _loaded_at                      AS dlt_load_id,
        -- _dlt_id is dlt's internal surrogate key. Child tables reference this.
        _dlt_id                         AS dlt_surrogate_key,
        -- When this dbt model was last materialised
        CURRENT_TIMESTAMP               AS dbt_updated_at

    FROM source
)

SELECT * FROM cleaned