#!/bin/sh
set -e

# Default to migrate if DB_STRATEGY is not set
DB_STRATEGY=${DB_STRATEGY:-migrate}

echo "Executing DB strategy: $DB_STRATEGY"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ "$DB_STRATEGY" = "migrate" ]; then
    (cd "$SCRIPT_DIR" && ./node_modules/.bin/sequelize-cli db:migrate --debug && echo migration completed successfully)
else
    echo "Unknown DB_STRATEGY: $DB_STRATEGY. Defaulting to migrate."
    (cd "$SCRIPT_DIR" && ./node_modules/.bin/sequelize-cli db:migrate --debug && echo migration completed successfully)
fi

echo "Starting application..."
exec node "$SCRIPT_DIR/dist/index.js"

