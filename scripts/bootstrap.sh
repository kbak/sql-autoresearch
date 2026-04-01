#!/usr/bin/env bash
set -euo pipefail

DB_NAME="${1:-autoresearch_test}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== sql-autoresearch test environment setup ==="
echo ""

# 1. Create database
if psql -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo "Database '$DB_NAME' already exists. Drop and recreate? [y/N]"
    read -r answer
    if [[ "$answer" == "y" || "$answer" == "Y" ]]; then
        dropdb "$DB_NAME"
        createdb "$DB_NAME"
        echo "Recreated database '$DB_NAME'"
    else
        echo "Keeping existing database"
    fi
else
    createdb "$DB_NAME"
    echo "Created database '$DB_NAME'"
fi

# 2. Load schema + data
echo ""
echo "Loading schema and generating data (this takes 2-4 minutes)..."
psql "$DB_NAME" < "$SCRIPT_DIR/setup_testdb.sql"

# 3. Register corpus queries
echo ""
echo "Registering corpus queries..."
cd "$PROJECT_DIR"

for sql_file in corpus/queries/q*.sql; do
    filename="$(basename "$sql_file")"
    # Extract description from first comment line
    desc="$(head -1 "$sql_file" | sed 's/^-- //' | LC_ALL=C tr -cd '[:print:]' | sed 's/"/\\"/g')"
    sha="$(shasum -a 256 "$sql_file" | cut -d' ' -f1)"
    echo "  $filename — $desc"
done

# Write manifest
cat > corpus/manifest.toml << 'MANIFEST_END'
MANIFEST_END

for sql_file in corpus/queries/q*.sql; do
    filename="$(basename "$sql_file")"
    desc="$(head -1 "$sql_file" | sed 's/^-- //' | LC_ALL=C tr -cd '[:print:]' | sed 's/"/\\"/g')"
    sha="$(shasum -a 256 "$sql_file" | cut -d' ' -f1)"
    cat >> corpus/manifest.toml << EOF
[[queries]]
file = "$filename"
sha256 = "$sha"
description = "$desc"

EOF
done

echo ""
echo "=== Setup complete ==="
echo ""
echo "Registered $(ls corpus/queries/q*.sql | wc -l | tr -d ' ') queries in corpus/manifest.toml"
echo ""
echo "Next steps:"
echo "  1. Check query support:"
echo "     uv run sql-autoresearch check --dsn 'postgresql://localhost/$DB_NAME'"
echo ""
echo "  2. Run the brutal test:"
echo "     export ANTHROPIC_API_KEY=sk-ant-..."
echo "     uv run sql-autoresearch run \\"
echo "       --dsn 'postgresql://localhost/$DB_NAME' \\"
echo "       --corpus corpus/ \\"
echo "       --accept-data-sent \\"
echo "       --quiescent-db"
