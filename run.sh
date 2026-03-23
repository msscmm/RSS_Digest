#!/bin/bash

export PATH=/usr/local/bin:/usr/bin:/bin

cd /home/melody_woo/rss_digest
mkdir -p logs 

set -e

echo "===== RUN START $(date) =====" >> logs/cron.log

# Step 1: run python（直接用venv）
/home/melody_woo/rss_digest/venv/bin/python digest_v13.py >> logs/digest.log 2>&1

echo "===== PYTHON DONE $(date) =====" >> logs/cron.log

# Step 2: git push（可选）
cd /home/melody_woo/rss_digest/output

git add . >> /home/melody_woo/rss_digest/logs/cron.log 2>&1 || true
git commit -m "auto update $(date '+%Y-%m-%d %H:%M')" >> /home/melody_woo/rss_digest/logs/cron.log 2>&1 || true
git pull --rebase >> /home/melody_woo/rss_digest/logs/cron.log 2>&1 || true
git push >> /home/melody_woo/rss_digest/logs/cron.log 2>&1 || true

echo "===== RUN END $(date) =====" >> /home/melody_woo/rss_digest/logs/cron.log
