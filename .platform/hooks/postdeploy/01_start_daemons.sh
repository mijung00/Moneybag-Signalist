#!/bin/bash
set -e
echo "🚀 [Post-Deploy Hook] Restarting custom daemons with new code..."
systemctl restart moralis-listener.service watchdog.service || echo "Failed to restart daemons, but continuing deployment."
echo "✅ [Post-Deploy Hook] Daemons restarted."