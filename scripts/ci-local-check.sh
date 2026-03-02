#!/bin/bash
# Local CI validation script
# Runs the same checks as GitHub Actions CI to ensure changes will pass
# Usage: ./scripts/ci-local-check.sh [OPTIONS] [PRESET]
#   OPTIONS:
#     --mode staged   Check only staged files (for pre-commit)
#     --mode branch   Check files changed in current branch vs main (for pre-push, default)
#     --mode all      Check all source files in the project
#     --tidy-only     Run only clang-tidy check, skip preset builds/tests
#     --no-cache      Disable clang-tidy cache
#   If PRESET is not specified, runs checks for all relevant presets

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
CLANG_TIDY_MODE="branch"  # Default to branch mode
USE_CACHE=1
CACHE_DIR=".cache/clang-tidy"
TIDY_ONLY=0

# Parse arguments
POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            CLANG_TIDY_MODE="$2"
            shift 2
            ;;
        --no-cache)
            USE_CACHE=0
            shift
            ;;
        --tidy-only)
            TIDY_ONLY=1
            shift
            ;;
        *)
            POSITIONAL_ARGS+=("$1")
            shift
            ;;
    esac
done
set -- "${POSITIONAL_ARGS[@]}"

echo -e "${GREEN}=== LeanStore Local CI Validation ===${NC}"
echo "Running in Docker container: $(cat /etc/os-release | grep PRETTY_NAME | cut -d'"' -f2)"
echo "Compiler: $(g++ --version | head -n1)"
echo "Clang-tidy mode: $CLANG_TIDY_MODE"
echo ""

# Default presets to check
DEFAULT_PRESETS=("debug_coro")

# If preset specified, use only that
if [ $# -ge 1 ]; then
    PRESETS=("$1")
else
    PRESETS=("${DEFAULT_PRESETS[@]}")
fi

# Function to run command with timing and status
run_check() {
    local name="$1"
    shift
    local command=("$@")
    
    echo -e "${YELLOW}▶ $name${NC}"
    echo "  Command: ${command[*]}"
    
    local start_time=$(date +%s)
    
    # Run the command
    if "${command[@]}"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        echo -e "${GREEN}  ✓ $name passed (${duration}s)${NC}"
        return 0
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        echo -e "${RED}  ✗ $name failed (${duration}s)${NC}"
        return 1
    fi
}

# Function to check a specific preset
check_preset() {
    local preset="$1"
    echo ""
    echo -e "${GREEN}=== Checking preset: $preset ===${NC}"
    
    # Configure
    if ! run_check "Configure $preset" cmake --preset "$preset"; then
        return 1
    fi
    
    # Cppcheck (only for presets that have it in CI)
    if [[ "$preset" == "debug_coro" ]]; then
        if ! run_check "Cppcheck" cppcheck --project="build/$preset/compile_commands.json" -i tests --error-exitcode=1 --check-level=exhaustive; then
            return 1
        fi
    fi
    
    # Clang-format check
    if ! run_check "Clang-format check" cmake --build "build/$preset" --target=check-format; then
        echo "  Note: Run 'cmake --build build/$preset --target format' to fix formatting"
        return 1
    fi
    
    # Build
    if ! run_check "Build $preset" cmake --build "build/$preset" -j "$(nproc)"; then
        return 1
    fi
    
    # Unit tests
    local test_cmd=("ctest" "--test-dir" "build/$preset" "--output-on-failure" "-j" "2")
    "${test_cmd[@]}"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}  ✓ Unit tests passed${NC}"
    else
        echo -e "${RED}  ✗ Unit tests failed${NC}"
        return 1
    fi
    
    return 0
}

# Function to compute file hash for caching
compute_file_hash() {
    local file="$1"
    if [ -f "$file" ]; then
        sha256sum "$file" | cut -d' ' -f1
    else
        echo ""
    fi
}

# Function to get cache key for a file (includes file hash + clang-tidy config hash)
get_cache_key() {
    local file="$1"
    local config_hash="$2"
    local file_hash=$(compute_file_hash "$file")
    echo "${file_hash}_${config_hash}"
}

# Function to check if file passes clang-tidy from cache
check_cache() {
    local file="$1"
    local cache_key="$2"
    local cache_file="$CACHE_DIR/$(echo "$file" | sed 's/[\/.]/_/g').cache"

    if [ -f "$cache_file" ]; then
        local cached_key=$(head -n1 "$cache_file")
        if [ "$cached_key" = "$cache_key" ]; then
            # Cache hit - return cached result (0=pass, 1=fail)
            local result=$(sed -n '2p' "$cache_file")
            return "$result"
        fi
    fi
    # Cache miss
    return 2
}

# Function to save result to cache
save_cache() {
    local file="$1"
    local cache_key="$2"
    local result="$3"
    local cache_file="$CACHE_DIR/$(echo "$file" | sed 's/[\/.]/_/g').cache"

    mkdir -p "$CACHE_DIR"
    echo "$cache_key" > "$cache_file"
    echo "$result" >> "$cache_file"
}

# Function to get files to check based on mode
get_files_to_check() {
    local mode="$1"
    local files=""

    case "$mode" in
        staged)
            # Get staged files (for pre-commit)
            files=$(git diff --cached --name-only --diff-filter=ACM 2>/dev/null | grep -E '\.(cpp|cc|c|h|hpp|hh)$' || true)
            ;;
        branch)
            # Get files changed in current branch vs main (for pre-push)
            # Try different base branch names (local main, origin/main, upstream/main)
            local base_branch=""
            for candidate in "main" "origin/main" "upstream/main" "leanstore/main"; do
                if git rev-parse --verify "$candidate" >/dev/null 2>&1; then
                    base_branch="$candidate"
                    break
                fi
            done

            if [ -z "$base_branch" ]; then
                echo -e "${YELLOW}⚠ Could not find main branch. Skipping branch diff check.${NC}" >&2
                files=""
            else
                # Try to find merge-base, fall back to direct diff if not possible
                local merge_base=$(git merge-base "$base_branch" HEAD 2>/dev/null || echo "$base_branch")
                files=$(git diff --name-only --diff-filter=ACM "$merge_base" HEAD 2>/dev/null | grep -E '\.(cpp|cc|c|h|hpp|hh)$' || true)
            fi
            ;;
        all)
            # Get all source files in the project (excluding examples/)
            files=$(find . -name "*.cpp" -o -name "*.cc" -o -name "*.c" -o -name "*.h" -o -name "*.hpp" -o -name "*.hh" | grep -v "./build" | grep -v "./.git" | grep -v "^./examples/" | sort)
            ;;
        *)
            echo -e "${RED}Unknown clang-tidy mode: $mode${NC}" >&2
            return 1
            ;;
    esac

    # Filter to only existing files and exclude examples/
    local existing_files=""
    for f in $files; do
        if [[ "$f" == examples/* ]]; then
            continue
        fi
        if [ -f "$f" ]; then
            existing_files="$existing_files $f"
        fi
    done

    echo "$existing_files"
}

# Function to run clang-tidy with incremental checking and caching
run_clang_tidy_check() {
    echo ""
    echo -e "${GREEN}=== Running Clang-Tidy Checks (mode: $CLANG_TIDY_MODE) ===${NC}"

    # We need a configured build directory for compile_commands.json
    local preset="debug_coro"

    if [ ! -d "build/$preset" ]; then
        echo "Configuring $preset preset first..."
        cmake --preset "$preset" > /dev/null 2>&1
    fi

    # Check if clang-tidy is available
    if ! command -v clang-tidy > /dev/null 2>&1; then
        echo -e "${YELLOW}⚠ clang-tidy not found. Skipping clang-tidy check.${NC}"
        echo "  Install with: sudo apt-get install clang-tidy"
        return 0
    fi

    # Get files to check based on mode
    local files_to_check=$(get_files_to_check "$CLANG_TIDY_MODE")

    if [ -z "$files_to_check" ]; then
        echo -e "${GREEN}✓ No C/C++ files to check in $CLANG_TIDY_MODE mode${NC}"
        return 0
    fi

    local file_count=$(echo "$files_to_check" | wc -w)
    echo "Found $file_count C/C++ file(s) to check"

    # Compute clang-tidy config hash for cache invalidation
    local config_hash=""
    if [ -f ".clang-tidy" ]; then
        config_hash=$(compute_file_hash ".clang-tidy")
    fi

    # Track results
    local files_checked=0
    local files_cached=0
    local files_failed=0
    local failed_files=""
    local all_output=""

    # First pass: filter out cached files
    local files_to_run=""
    for file in $files_to_check; do
        local cache_key=$(get_cache_key "$file" "$config_hash")

        # Check cache first (if enabled)
        if [ "$USE_CACHE" -eq 1 ]; then
            check_cache "$file" "$cache_key"
            local cache_result=$?

            if [ "$cache_result" -eq 0 ]; then
                # Cache hit - file passed before
                ((files_cached++)) || true
                continue
            fi
            # cache_result == 1 or 2 means need to check
        fi

        files_to_run="$files_to_run $file"
    done

    # If no files to run, we're done
    if [ -z "$files_to_run" ]; then
        echo ""
        echo "Results: 0 checked, $files_cached cached (skipped), 0 failed"
        echo -e "${GREEN}✓ Clang-tidy passed${NC}"
        return 0
    fi

    local files_to_run_count=$(echo "$files_to_run" | wc -w)
    local num_jobs=$(nproc)
    echo "Running clang-tidy on $files_to_run_count file(s) with $num_jobs parallel jobs..."

    # Create temp directory for outputs
    local tmp_dir=$(mktemp -d)
    trap "rm -rf $tmp_dir" EXIT

    # Create a helper script for parallel execution
    local helper_script="$tmp_dir/run_clang_tidy.sh"
    cat > "$helper_script" << 'HELPER_EOF'
#!/bin/bash
file="$1"
preset="$2"
tmp_dir="$3"
out_file="$tmp_dir/$(echo "$file" | md5sum | cut -d' ' -f1).out"

# Determine language standard based on extension
if [[ "$file" == *.c ]]; then
    STD_ARG="--extra-arg=-std=c11"
else
    STD_ARG="--extra-arg=-std=c++2b"
fi

clang-tidy \
    -p="build/$preset" \
    --config-file=".clang-tidy" \
    $STD_ARG \
    --extra-arg=-Wno-unknown-warning-option \
    --extra-arg=-Wno-error=clobbered \
    "$file" 2>&1 > "$out_file"
# Store the original filename for later lookup
echo "$file" > "$out_file.name"
HELPER_EOF
    chmod +x "$helper_script"

    # Run clang-tidy in parallel using xargs
    echo "$files_to_run" | tr ' ' '\n' | grep -v '^$' | \
        xargs -P "$num_jobs" -I {} "$helper_script" {} "$preset" "$tmp_dir" 2>/dev/null || true

    # Process results and update cache
    for name_file in "$tmp_dir"/*.name; do
        [ -f "$name_file" ] || continue
        local file=$(cat "$name_file")
        local out_file="${name_file%.name}"

        ((files_checked++)) || true
        local cache_key=$(get_cache_key "$file" "$config_hash")

        if [ -f "$out_file" ]; then
            local output=$(cat "$out_file")

            # Check for warnings/errors
            if echo "$output" | grep -qE "warning:|error:"; then
                ((files_failed++)) || true
                failed_files="$failed_files $file"
                all_output="$all_output\n=== $file ===\n$output"

                # Save failure to cache
                if [ "$USE_CACHE" -eq 1 ]; then
                    save_cache "$file" "$cache_key" "1"
                fi
            else
                # Save success to cache
                if [ "$USE_CACHE" -eq 1 ]; then
                    save_cache "$file" "$cache_key" "0"
                fi
            fi
        fi
    done

    # Report results
    echo ""
    echo "Results: $files_checked checked, $files_cached cached (skipped), $files_failed failed"

    if [ "$files_failed" -gt 0 ]; then
        echo -e "${RED}✗ Clang-tidy found issues in $files_failed file(s):${NC}"
        for f in $failed_files; do
            echo "  - $f"
        done
        echo ""
        echo "Details:"
        echo -e "$all_output" | grep -E "warning:|error:" | head -30
        return 1
    else
        echo -e "${GREEN}✓ Clang-tidy passed${NC}"
        return 0
    fi
}

# Main execution
FAILED=0

# Run clang-tidy check first (since it's often the cause of failures)
if ! run_clang_tidy_check; then
    FAILED=1
fi

# If --tidy-only is set, skip preset checks
if [ "$TIDY_ONLY" -eq 1 ]; then
    if [ $FAILED -eq 0 ]; then
        echo -e "${GREEN}✅ Clang-tidy check passed!${NC}"
    else
        echo -e "${RED}❌ Clang-tidy check failed.${NC}"
    fi
    exit $FAILED
fi

# Check each preset
for preset in "${PRESETS[@]}"; do
    if check_preset "$preset"; then
        echo -e "${GREEN}✓ Preset $preset: ALL CHECKS PASSED${NC}"
    else
        echo -e "${RED}✗ Preset $preset: SOME CHECKS FAILED${NC}"
        FAILED=1
    fi
done

echo ""
echo -e "${GREEN}=== Summary ===${NC}"

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✅ All checks passed! Your changes should pass CI.${NC}"
    echo ""
    echo "Recommended next steps:"
    echo "  1. Commit your changes with a conventional commit message"
    echo "  2. Push to your branch"
    echo "  3. Create/update PR if needed"
    echo "  4. Monitor GitHub Actions for final validation"
else
    echo -e "${RED}❌ Some checks failed. Please fix the issues before committing.${NC}"
    echo ""
    echo "Common issues and fixes:"
    echo "  • Formatting: Run 'cmake --build build/debug_coro --target format'"
    echo "  • Clang-tidy: Check .clang-tidy rules (naming, style, etc.)"
    echo "  • Build errors: Check compiler output above"
    echo "  • Test failures: Run specific test with 'ctest --test-dir build/PRESET -R TESTNAME'"
fi

exit $FAILED
