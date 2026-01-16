#!/bin/bash
# Install LeanStore git hooks for automated quality checks

set -e

echo "Installing LeanStore git hooks..."

# Check if we're in a git repository
if [ ! -d ".git" ]; then
    echo "Error: Not a git repository"
    exit 1
fi

HOOKS_DIR=".git/hooks"
SCRIPTS_DIR="$(dirname "$0")"

# List of hooks to install
HOOKS=("commit-msg" "pre-commit" "pre-push")

# Copy each hook if it exists in scripts directory
for hook in "${HOOKS[@]}"; do
    SRC="$SCRIPTS_DIR/git-hooks/$hook"
    DEST="$HOOKS_DIR/$hook"
    
    # If hook exists in scripts/git-hooks/, copy it
    if [ -f "$SRC" ]; then
        cp "$SRC" "$DEST"
        chmod +x "$DEST"
        echo "  ✓ Installed $hook hook"
    else
        # Check if hook already exists in .git/hooks
        if [ -f "$DEST" ] && [ ! -f "$DEST.sample" ]; then
            echo "  • $hook hook already exists (not overwriting)"
        else
            echo "  • $hook hook not found in scripts/git-hooks/"
        fi
    fi
done

# Check for post-push hook (special case - already exists)
if [ -f "$HOOKS_DIR/post-push" ]; then
    echo "  • post-push hook already exists"
else
    # Create a simple post-push hook if it doesn't exist
    cat > "$HOOKS_DIR/post-push" << 'EOF'
#!/bin/sh
branch=$(git rev-parse --abbrev-ref HEAD)
url=$(gh pr view --json url -q ".url" 2>/dev/null || true)
if [ ! -z "$url" ]; then
  echo "View pull request: $url"
fi
EOF
    chmod +x "$HOOKS_DIR/post-push"
    echo "  ✓ Created post-push hook"
fi

echo ""
echo "Git hooks installation complete."
echo ""
echo "The following hooks are now active:"
echo "  • commit-msg    - Validates conventional commits format"
echo "  • pre-commit    - Quick code quality checks"
echo "  • pre-push      - Full CI validation before pushing"
echo "  • post-push     - Shows PR URL after pushing"
echo ""
echo "To skip hooks temporarily:"
echo "  • Commit: git commit --no-verify"
echo "  • Push:   LEANSTORE_SKIP_PREPUSH=1 git push"
echo ""
echo "To uninstall hooks, remove files from .git/hooks/"