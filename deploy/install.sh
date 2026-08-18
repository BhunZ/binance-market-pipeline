#!/usr/bin/env bash
# Install and start the two streaming services on the VM.
#
# Idempotent: run it again after a code change and it reloads and restarts rather than
# duplicating anything. It does not write .bmp_env — credentials are placed by hand, once, so a
# deploy script can never overwrite them with blanks.
set -euo pipefail

REPO="$HOME/binance-market-pipeline"
ENV_FILE="$HOME/.bmp_env"

[ -f "$ENV_FILE" ] || { echo "missing $ENV_FILE — create it before installing"; exit 1; }

# systemd refuses to start a unit whose ReadWritePaths does not exist. This one is only used
# when object storage is unconfigured, so it is usually absent on a working host.
mkdir -p "$REPO/data"

for unit in bmp-ws-consumer bmp-aggregator; do
    sudo cp "$REPO/deploy/$unit.service" "/etc/systemd/system/$unit.service"
done

sudo systemctl daemon-reload
sudo systemctl enable --now bmp-ws-consumer bmp-aggregator

sleep 5
for unit in bmp-ws-consumer bmp-aggregator; do
    printf '  %-20s %s\n' "$unit" "$(systemctl is-active "$unit")"
done
