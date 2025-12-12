#!/usr/bin/env bash
set -euo pipefail

# Function: main
# Purpose: Orchestrates argument parsing, file discovery for a PR, filters C/C++ files,
#          and runs clang-tidy on the changed files.
# Inputs:
#   $1: PR number (string/integer)
#   $2: GitHub repo in the form "owner/repo"
#   $3: GitHub token with repo read permissions
#   $4: Build path where compile_commands.json exists (e.g., build/debug)
# Outputs:
#   Prints changed files and clang-tidy output to stdout; returns non-zero on unexpected failures.
main() {
  parse_args "$@"

  local all_files
  all_files=$(fetch_all_changed_files "$PR_NUMBER" "$REPO" "$TOKEN")

  local changed_files
  changed_files=$(filter_cc_files "$all_files")

  echo "Changed C/C++ files:"
  echo "$changed_files"

  if [[ -z "$changed_files" ]]; then
    echo "No C/C++ files changed. Skipping clang-tidy."
    return 0
  fi

  run_clang_tidy_on_files "$changed_files" "$BUILD_PATH"
}

# Function: parse_args
# Purpose: Validates and assigns positional arguments to global variables.
# Inputs:
#   $1: PR number
#   $2: repo (owner/repo)
#   $3: token
#   $4: build path
# Outputs:
#   Sets globals: PR_NUMBER, REPO, TOKEN, BUILD_PATH. Exits with usage on missing args.
parse_args() {
  if [[ $# -lt 4 ]]; then
    echo "Usage: $0 <PR_NUMBER> <OWNER/REPO> <GITHUB_TOKEN> <BUILD_PATH>" >&2
    exit 1
  fi
  PR_NUMBER="$1"
  REPO="$2"
  TOKEN="$3"
  BUILD_PATH="$4"
}

# Function: run_clang_tidy_on_files
# Purpose: Runs clang-tidy for a whitespace-separated list of files using the given build path.
# Inputs:
#   $1: whitespace-separated list of file paths
#   $2: build path for compile_commands.json
# Outputs:
#   Prints clang-tidy output; returns clang-tidy exit code.
run_clang_tidy_on_files() {
  local files="$1"
  local build_path="$2"
  echo "Running clang-tidy on changed files..."
  run-clang-tidy \
    -p="$build_path" \
    -config-file=".clang-tidy" \
    -j "$(nproc)" \
    -quiet \
    -extra-arg=-std=c++2b \
    -extra-arg=-Wno-unknown-warning-option \
    $files
}

# Function: filter_cc_files
# Purpose: Filters input newline-separated file list to only C/C++ relevant extensions.
# Inputs:
#   $1: newline-separated file paths
# Outputs:
#   Echoes whitespace-separated filtered C/C++ file paths (extensions: c, cc, cpp, h, hh, hpp).
filter_cc_files() {
  local all_files="$1"
  echo "$all_files" | grep -E '\\.(cpp|cc|c|h|hpp|hh)$' || true
}

# Function: fetch_all_changed_files
# Purpose: Fetches all changed files in a PR via GitHub API v3, following pagination.
# Inputs:
#   $1: PR number
#   $2: repo (owner/repo)
#   $3: token
# Outputs:
#   Echoes a newline-separated list of file paths.
fetch_all_changed_files() {
  local pr_number="$1"
  local repo="$2"
  local token="$3"

  local page_url="https://api.github.com/repos/${repo}/pulls/${pr_number}/files?per_page=100"
  local all_files=""

  while [[ -n "$page_url" ]]; do
    # Perform request and capture headers + body
    local resp
    resp=$(curl -s -i \
      -H "Authorization: token ${token}" \
      -H "Accept: application/vnd.github+json" \
      "$page_url")

    # Extract body (after first blank line) and collect filenames
    local body
    body=$(echo "$resp" | sed -n '/^\r$/,$p' | sed '1d')
    local files
    files=$(echo "$body" | grep '"filename":' | sed -E 's/.*"filename": "([^"]+)".*/\1/')
    if [[ -n "$files" ]]; then
      # Keep as newline-separated list
      all_files+=$'\n'"$files"
    fi

    # Parse Link header for pagination
    local link
    link=$(echo "$resp" | grep -i '^Link:' || true)

    local next_url=""
    if echo "$link" | grep -q 'rel="next"'; then
      next_url=$(echo "$link" | sed -E 's/.*<([^>]+)>; rel="next".*/\1/')
    fi
    page_url="$next_url"
  done

  # Trim leading newline if present and echo
  echo -e "$all_files" | sed '/^$/d'
}

# Entry point
main "$@"
