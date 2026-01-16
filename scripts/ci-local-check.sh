#!/bin/bash
# Local CI validation script
# Runs the same checks as GitHub Actions CI to ensure changes will pass
# Usage: ./scripts/ci-local-check.sh [PRESET]
#   If PRESET is not specified, runs checks for all relevant presets

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== LeanStore Local CI Validation ===${NC}"
echo "Running in Docker container: $(cat /etc/os-release | grep PRETTY_NAME | cut -d'"' -f2)"
echo "Compiler: $(g++ --version | head -n1)"
echo ""

# Default presets to check
DEFAULT_PRESETS=("debug_tsan" "debug_cov" "debug_coro")

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
    if [[ "$preset" == "debug_cov" || "$preset" == "debug_coro" ]]; then
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
    
    # Unit tests with appropriate options
    local test_cmd=("ctest" "--test-dir" "build/$preset" "--output-on-failure" "-j" "2")
    if [[ "$preset" == "debug_tsan" || "$preset" == "debug_cov" ]]; then
        # Add TSAN options for these presets
        TSAN_OPTIONS="suppressions=$(pwd)/tests/tsan.supp" "${test_cmd[@]}"
    else
        "${test_cmd[@]}"
    fi
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}  ✓ Unit tests passed${NC}"
    else
        echo -e "${RED}  ✗ Unit tests failed${NC}"
        return 1
    fi
    
    return 0
}

# Function to run clang-tidy (simplified local version)
run_clang_tidy_check() {
    echo ""
    echo -e "${GREEN}=== Running Clang-Tidy Checks ===${NC}"
    
    # We need a configured build directory for compile_commands.json
    # Use debug_tsan as it's what the CI uses for regular clang-tidy
    local preset="debug_tsan"
    
    if [ ! -d "build/$preset" ]; then
        echo "Configuring $preset preset first..."
        cmake --preset "$preset" > /dev/null 2>&1
    fi
    
    # Check if run-clang-tidy is available
    if ! command -v run-clang-tidy > /dev/null 2>&1; then
        echo -e "${YELLOW}⚠ run-clang-tidy not found. Skipping clang-tidy check.${NC}"
        echo "  Install with: sudo apt-get install clang-tidy"
        return 0
    fi
    
    # Simplified clang-tidy check on all C++ files (not just changed ones like in CI)
    echo "Running clang-tidy on all C++ files..."
    
    # Find all C++ files
    CPP_FILES=$(find . -name "*.cpp" -o -name "*.cc" -o -name "*.c" -o -name "*.h" -o -name "*.hpp" -o -name "*.hh" \
        | grep -v "./build/" | grep -v "./vcpkg_installed/" | grep -v "./.cache/" | sort)
    
    echo "Found $(echo "$CPP_FILES" | wc -l) C/C++ files"
    
    # Run clang-tidy with same options as CI script
    if run-clang-tidy \
        -p="build/$preset" \
        -config-file=".clang-tidy" \
        -j "$(nproc)" \
        -quiet \
        -extra-arg=-std=c++2b \
        -extra-arg=-Wno-unknown-warning-option \
        -extra-arg=-Wno-error=clobbered \
        $CPP_FILES 2>&1 | tee /tmp/clang-tidy-output.txt; then
        
        # Check if there were any warnings/errors
        if grep -q "warning:\|error:" /tmp/clang-tidy-output.txt; then
            echo -e "${RED}✗ Clang-tidy found issues${NC}"
            grep -E "warning:|error:" /tmp/clang-tidy-output.txt | head -20
            return 1
        else
            echo -e "${GREEN}✓ Clang-tidy passed (no warnings)${NC}"
            return 0
        fi
    else
        echo -e "${RED}✗ Clang-tidy command failed${NC}"
        return 1
    fi
}

# Main execution
FAILED=0

# Run clang-tidy check first (since it's often the cause of failures)
if ! run_clang_tidy_check; then
    FAILED=1
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