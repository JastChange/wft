#!/usr/bin/env bash
# Read-only inspection script: report system load averages.
set -uo pipefail
cat /proc/loadavg 2>/dev/null || uptime
