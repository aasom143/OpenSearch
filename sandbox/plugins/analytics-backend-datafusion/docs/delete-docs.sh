#!/bin/bash
# Delete documents from clickbench index by fetching IDs and bulk-deleting them.
# Usage: ./delete-docs.sh [HOST] [INDEX] [QUERY_JSON] [BATCH_SIZE]
#
# Example:
#   ./delete-docs.sh localhost:9200 clickbench '{"query":{"bool":{"must":[{"match":{"Title":"google"}},{"term":{"Age":31}}]}}}' 5000

HOST="${1:-localhost:9200}"
INDEX="${2:-clickbench}"
QUERY="${3:-{\"query\":{\"match_all\":{}}}}"
BATCH_SIZE="${4:-5000}"

TOTAL_DELETED=0

echo "=== Delete Docs Script ==="
echo "Host: $HOST"
echo "Index: $INDEX"
echo "Batch size: $BATCH_SIZE"
echo "Query: $QUERY"
echo ""

while true; do
    # Step 1: Fetch doc IDs
    RESPONSE=$(curl -s "$HOST/$INDEX/_search?size=$BATCH_SIZE&_source=false" \
        -H "Content-Type: application/json" \
        -d "$QUERY")

    # Extract IDs from response
    IDS=$(echo "$RESPONSE" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    hits = data.get('hits', {}).get('hits', [])
    for hit in hits:
        print(hit['_id'])
except Exception as e:
    print(f'ERROR: {e}', file=sys.stderr)
    sys.exit(1)
" 2>/dev/null)

    if [ $? -ne 0 ] || [ -z "$IDS" ]; then
        echo "No more documents to delete (or search failed)."
        echo "Response: $RESPONSE" | head -5
        break
    fi

    COUNT=$(echo "$IDS" | wc -l | tr -d ' ')
    echo "Found $COUNT documents to delete..."

    # Step 2: Build bulk delete request
    BULK_BODY=""
    while IFS= read -r DOC_ID; do
        BULK_BODY+='{"delete":{"_index":"'"$INDEX"'","_id":"'"$DOC_ID"'"}}'$'\n'
    done <<< "$IDS"

    # Step 3: Execute bulk delete
    BULK_RESPONSE=$(curl -s -X POST "$HOST/_bulk" \
        -H "Content-Type: application/json" \
        -d "$BULK_BODY")

    # Check for errors
    HAS_ERRORS=$(echo "$BULK_RESPONSE" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    errors = data.get('errors', False)
    if errors:
        failed = [item for item in data.get('items', []) if item.get('delete', {}).get('status', 200) >= 400]
        print(f'{len(failed)} failures')
    else:
        print('ok')
except:
    print('parse_error')
" 2>/dev/null)

    TOTAL_DELETED=$((TOTAL_DELETED + COUNT))
    echo "  Deleted batch: $COUNT (total: $TOTAL_DELETED) - bulk status: $HAS_ERRORS"

    # If we got fewer than batch size, we're done
    if [ "$COUNT" -lt "$BATCH_SIZE" ]; then
        break
    fi

    # Small pause to avoid overwhelming the cluster
    sleep 0.5
done

echo ""
echo "=== Done. Total deleted: $TOTAL_DELETED ==="
