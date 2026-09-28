#!/usr/bin/env bash
#
# Thin wrapper that runs the structured, evidence-producing failure suite.
# The real work lives in run-failure-suite.mjs.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

node "${SCRIPT_DIR}/run-failure-suite.mjs"

