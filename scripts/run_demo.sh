#!/bin/sh
set -eu

python src/generate_orders.py --output-dir data/raw
python src/lakehouse.py
python src/check_quality.py --expect-default
python -m unittest discover -s tests -v
