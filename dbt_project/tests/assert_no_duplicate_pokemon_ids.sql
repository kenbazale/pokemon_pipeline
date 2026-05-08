-- Custom test: returns rows only when the test FAILS.
-- dbt considers the test failed if this query returns any rows.
-- This tests the grain of dim_pokemon more explicitly than the generic 'unique' test.

SELECT
    pokemon_id,
    COUNT(*) AS occurrences
FROM {{ ref('dim_pokemon') }}
GROUP BY pokemon_id
HAVING COUNT(*) > 1