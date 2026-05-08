"""
Unit tests for dlt source logic.

We test the GENERATOR functions in isolation — no actual API calls.
The key is testing that:
  1. Records have the correct shape.
  2. Incremental state filtering works.
  3. Audit columns are present.
"""
from unittest.mock import patch, MagicMock
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dlt_pipelines"))
from pokemon_source import pokemon_detail_resource


MOCK_LIST_RESPONSE = {
    "count": 2,
    "results": [
        {"name": "bulbasaur", "url": "https://pokeapi.co/api/v2/pokemon/1/"},
        {"name": "ivysaur",   "url": "https://pokeapi.co/api/v2/pokemon/2/"},
    ]
}

MOCK_DETAIL_BULBASAUR = {
    "id": 1, "name": "bulbasaur", "order": 1, "is_default": True,
    "height": 7, "weight": 69, "base_experience": 64,
    "abilities": [{"ability": {"name": "overgrow"}, "is_hidden": False}],
    "types":     [{"type": {"name": "grass"}, "slot": 1}],
    "stats":     [{"base_stat": 45, "stat": {"name": "hp"}}],
    "sprites":   {"front_default": "https://..."},
}

MOCK_DETAIL_IVYSAUR = {**MOCK_DETAIL_BULBASAUR, "id": 2, "name": "ivysaur"}


@patch("pokemon_source.requests.get")
def test_detail_resource_yields_correct_shape(mock_get):
    """Verify that yielded records have required fields."""
    list_resp   = MagicMock(); list_resp.json.return_value = MOCK_LIST_RESPONSE
    detail_resp_1 = MagicMock(); detail_resp_1.json.return_value = MOCK_DETAIL_BULBASAUR
    detail_resp_2 = MagicMock(); detail_resp_2.json.return_value = MOCK_DETAIL_IVYSAUR
    mock_get.side_effect = [list_resp, detail_resp_1, detail_resp_2]

    records = list(pokemon_detail_resource(max_pokemon=2))

    assert len(records) == 2
    first = records[0]
    assert first["id"] == 1
    assert first["name"] == "bulbasaur"
    assert "height_dm" in first
    assert "weight_hg" in first
    assert "abilities" in first
    assert "_loaded_at" in first


@patch("pokemon_source.requests.get")
def test_detail_resource_incremental_skips_seen_ids(mock_get):
    """Verify that records with ID <= last_seen_id are skipped."""
    list_resp   = MagicMock(); list_resp.json.return_value = MOCK_LIST_RESPONSE
    empty_list_resp = MagicMock(); empty_list_resp.json.return_value = {"count": 2, "results": []}
    detail_resp_1 = MagicMock(); detail_resp_1.json.return_value = MOCK_DETAIL_BULBASAUR
    detail_resp_2 = MagicMock(); detail_resp_2.json.return_value = MOCK_DETAIL_IVYSAUR
    mock_get.side_effect = [list_resp, detail_resp_1, detail_resp_2, empty_list_resp]

    # Simulate a previous run that already loaded id=1
    # By setting initial_value=1, the resource should skip bulbasaur (id=1)
    import dlt
    resource = pokemon_detail_resource(
        pokemon_ids=dlt.sources.incremental("id", initial_value=1),
        max_pokemon=2
    )
    records = list(resource)
    ids = [r["id"] for r in records]

    assert 1 not in ids   # bulbasaur already seen
    assert 2 in ids       # ivysaur is new