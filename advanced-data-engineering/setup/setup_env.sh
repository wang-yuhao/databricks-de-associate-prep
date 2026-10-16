#!/usr/bin/env bash
# One-time setup of the local practice environment (macOS / Linux).
#   bash setup/setup_env.sh
# Requires python3.11 and a JDK 17 on PATH (or JAVA_HOME).
set -euo pipefail
cd "$(dirname "$0")/.."

[ -x .venv/bin/python ] || python3.11 -m venv .venv
PY=.venv/bin/python
$PY -m pip install --upgrade pip --quiet
$PY -m pip install -r requirements.txt -e . --quiet
$PY -m ipykernel install --user --name adelab --display-name "Python (adelab)"
$PY -c "from adelab.spark import resolve_jars; print(len(resolve_jars()), 'Delta Lake jars ready')"
$PY -m adelab.doctor
echo "Start the lab with: .venv/bin/jupyter lab"
