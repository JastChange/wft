#!/usr/bin/env bash
# Read-only inspection script: report disk usage by mount point.
set -uo pipefail
df -h --output=target,used,avail,pcent 2>/dev/null || df -h
