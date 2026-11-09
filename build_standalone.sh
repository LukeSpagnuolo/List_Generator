#!/bin/zsh
# Rebuild List_Generator_Standalone/ (the shareable copy) from the files in this folder.
set -e

SCRIPT_DIR="${0:A:h}"
cd "$SCRIPT_DIR"
OUT="List_Generator_Standalone"

FILES=(
    list_generator_7000.py
    prebuilt_reports_7000.py
    national_inventory_workflow.py
    conversion_update_workflow.py
    requirements.txt
    run_list_generator_7000.command
    .env.example
    Cities_Extended_Mapped.csv
    assets/EE_Sport_Template_Blank.xlsx
)

mkdir -p "$OUT/assets"
for file in $FILES; do
    cp "$file" "$OUT/$file"
done
cp STANDALONE_README.txt "$OUT/README.txt"
chmod +x "$OUT/run_list_generator_7000.command"

echo "Rebuilt $OUT from the current project files."
