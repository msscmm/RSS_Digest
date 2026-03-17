#!/bin/bash

cd /home/melody_woo/rss_digest

source venv/bin/activate

python digest_v9.py >> logs/digest.log 2>&1

deactivate

# === Git push ===
cd /home/melody_woo/rss_digest/output

git add .

git commit -m "auto update $(date '+%Y-%m-%d %H:%M')" || echo "No changes"

git push
