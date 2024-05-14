from pathlib import Path


COLLECTION_SCHEMA_PATH = (
    Path(__file__).parent
    / "schemas"
    / "collection-spec"
    / "json-schema"
    / "collection.json"
)
assert COLLECTION_SCHEMA_PATH.exists()
ITEM_SCHEMA_PATH = (
    Path(__file__).parent / "schemas" / "item-spec" / "json-schema" / "item.json"
)
assert ITEM_SCHEMA_PATH.exists()
