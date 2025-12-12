PR_NUMBER="$1"
REPO="$2"
TOKEN="$3"
BUILD_PATH="$4"

PAGE_URL="https://api.github.com/repos/$REPO/pulls/$PR_NUMBER/files?per_page=100"
ALL_FILES=""
while [[ -n "$PAGE_URL" ]]; do
  resp=$(curl -s -i \
    -H "Authorization: token $TOKEN" \
    -H "Accept: application/vnd.github+json" \
    "$PAGE_URL")

  body=$(echo "$resp" | sed -n '/^\r$/,$p' | sed '1d')
  files=$(echo "$body" | grep '"filename":' | sed -E 's/.*"filename": "([^"]+)".*/\1/')
  ALL_FILES="$ALL_FILES"$'\n'"$files"

  link=$(echo "$resp" | grep -i '^Link:' || true)

  NEXT_URL=""
  if echo "$link" | grep -q 'rel="next"'; then
    NEXT_URL=$(echo "$link" | sed -E 's/.*<([^>]+)>; rel="next".*/\1/')
  fi
  PAGE_URL="$NEXT_URL"
done

CHANGED_FILES=$(echo "$ALL_FILES" | grep -E '\.(cpp|cc|c|h|hpp|hh)$' || true)
echo "Changed C/C++ files:"
echo "$CHANGED_FILES"

if [ -z "$CHANGED_FILES" ]; then
  echo "No C/C++ files changed. Skipping clang-tidy."
  exit 0
fi

echo "Running clang-tidy on changed files..."
run-clang-tidy \
  -p="$BUILD_PATH" \
  -config-file=".clang-tidy" \
  -j $(nproc) \
  -quiet \
  -extra-arg=-std=c++2b \
  -extra-arg=-Wno-unknown-warning-option \
  $CHANGED_FILES
