#!/bin/bash
echo "--- [Predeploy Hook] Installing Chromium and fonts for html2image ---"
amazon-linux-extras install -y chromium
yum install -y liberation-sans-narrow-fonts
echo "--- Chromium and fonts installation finished. ---"