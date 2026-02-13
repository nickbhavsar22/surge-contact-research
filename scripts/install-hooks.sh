#!/bin/bash
# Install git hooks for this project.
# Run once after cloning: bash scripts/install-hooks.sh

HOOK_DIR="$(git rev-parse --show-toplevel)/.git/hooks"

cat > "$HOOK_DIR/pre-commit" << 'HOOK'
#!/bin/bash
# Auto-increment patch version in app.py on every commit.
# Format: MAJOR.MINOR.PATCH  — only PATCH is bumped automatically.
# To bump MAJOR or MINOR, edit app.py manually before committing.

APP_FILE="app.py"

# Only run if app.py exists in the repo
if [ ! -f "$APP_FILE" ]; then
    exit 0
fi

# Extract current version
CURRENT=$(grep -oP '__version__\s*=\s*"\K[0-9]+\.[0-9]+\.[0-9]+' "$APP_FILE")

if [ -z "$CURRENT" ]; then
    echo "pre-commit: could not parse __version__ from $APP_FILE, skipping auto-increment"
    exit 0
fi

# Split into MAJOR.MINOR.PATCH
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"
PATCH=$((PATCH + 1))
NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"

# Replace in-place
sed -i "s/__version__ = \"${CURRENT}\"/__version__ = \"${NEW_VERSION}\"/" "$APP_FILE"

# Stage the updated file so the new version is included in the commit
git add "$APP_FILE"

echo "pre-commit: version bumped ${CURRENT} -> ${NEW_VERSION}"
HOOK

chmod +x "$HOOK_DIR/pre-commit"
echo "Git hooks installed successfully."
