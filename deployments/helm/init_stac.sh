#!/bin/bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "type": "Collection",
    "id": "my-collection",
    "description": "A minimal sample collection",
    "license": "CC-BY-4.0",
    "extent": {
      "spatial": {
        "bbox": [
          [-180, -90, 180, 90]
        ]
      },
      "temporal": {
        "interval": [
          ["2023-01-01T00:00:00Z", "2024-12-31T23:59:59Z"]
        ]
      }
    }
  }' \
  http://localhost:8080/collections