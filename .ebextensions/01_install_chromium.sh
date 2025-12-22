#!/bin/bash
set -e # 한 명령어라도 실패하면 즉시 중단

LOG_FILE="/var/log/chromium_install.log"
# 스크립트 시작 시 로그 파일을 먼저 생성하고 권한을 부여하여 로그 누락 방지
touch $LOG_FILE
chmod 666 $LOG_FILE

echo "--- [$(date)] Starting Chromium Installation Script ---" > $LOG_FILE

ARCH=$(uname -m)
echo "Architecture detected: $ARCH" >> $LOG_FILE

echo "Installing common dependencies (including unzip)..." >> $LOG_FILE
sudo yum install -y alsa-lib atk at-spi2-atk cups-libs gtk3 ipa-gothic-fonts libXcomposite libXcursor libXdamage libXext libXi libXrandr libXScrnSaver libXtst pango xorg-x11-fonts-Type1 xorg-x11-font-utils libgbm unzip -y >> $LOG_FILE 2>&1

if [ "$ARCH" = "aarch64" ]; then
  echo "ARM (aarch64) flow selected." >> $LOG_FILE
  cd /tmp
  echo "Downloading pre-built Chromium for ARM..." >> $LOG_FILE
  curl -Lo chromium.zip https://github.com/Sparticuz/chromium/releases/download/v123.0.1/chromium-v123.0.1-pack.zip >> $LOG_FILE 2>&1
  
  echo "Unzipping Chromium..." >> $LOG_FILE
  unzip -o chromium.zip >> $LOG_FILE 2>&1
  
  echo "Moving Chromium to /opt/chromium..." >> $LOG_FILE
  sudo rm -rf /opt/chromium
  sudo mv pack /opt/chromium >> $LOG_FILE 2>&1
  
  echo "Creating symbolic links for ARM..." >> $LOG_FILE
  sudo ln -sf /opt/chromium/chrome /usr/bin/chromium
  sudo ln -sf /opt/chromium/chrome /usr/bin/chromium-browser
else
  echo "x86_64 flow selected." >> $LOG_FILE
  echo "Enabling EPEL repository..." >> $LOG_FILE
  sudo amazon-linux-extras install epel -y >> $LOG_FILE 2>&1 || echo "EPEL already enabled or failed, continuing..." >> $LOG_FILE
  
  echo "Installing Chromium via yum..." >> $LOG_FILE
  sudo yum install -y chromium >> $LOG_FILE 2>&1
  
  echo "Creating symbolic link for x86_64..." >> $LOG_FILE
  sudo ln -sf /usr/bin/chromium-browser /usr/bin/chromium
fi

echo "Verifying installation..." >> $LOG_FILE
if [ -L /usr/bin/chromium ]; then
    echo "✅ SUCCESS: Symlink /usr/bin/chromium created." >> $LOG_FILE
    ls -l /usr/bin/chromium >> $LOG_FILE
    /usr/bin/chromium --version >> $LOG_FILE 2>&1
else
    echo "❌ ERROR: Symlink /usr/bin/chromium was NOT created." >> $LOG_FILE
    exit 1
fi

echo "--- [$(date)] Chromium Installation Script Finished ---" >> $LOG_FILE