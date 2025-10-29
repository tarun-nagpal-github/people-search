#!/usr/bin/env bash
#cd /opt/intranet

echo "OpenSearch settings migrations triggered"
export PYTHONWARNINGS="ignore"
export PYTHONPATH=.
python3 opensearch_settings/migrations.py
echo "OpenSearch settings migrations completed"