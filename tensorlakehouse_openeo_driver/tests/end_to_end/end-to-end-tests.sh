#!/usr/bin/env bash

pytest -vv tensorlakehouse_openeo_driver/tests/end_to_end/ --report-log=end-to-end-test-results.json
