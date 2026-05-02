#!/bin/bash
# Build mcpproxy from local source
# Location: ~/workspace/mcpproxy (shared across all Cursor workspaces)
#
# Usage:
#   ./build-local.sh              # Build only
#   ./build-local.sh --install    # Build and install to ~/bin
#   ./build-local.sh --update     # Fetch latest tag, build, and install
#
# Requirements:
#   - Go 1.21+ (install via: mise use -g go@latest)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Handle --update flag
if [ "$1" == "--update" ]; then
    echo "📡 Fetching latest tags..."
    git fetch --tags
    LATEST_TAG=$(git describe --tags --abbrev=0 origin/main 2>/dev/null || git tag --sort=-v:refname | head -1)
    echo "📦 Checking out $LATEST_TAG..."
    git checkout "$LATEST_TAG"
    shift  # Remove --update so --install check still works
    INSTALL_FLAG="--install"
else
    INSTALL_FLAG="$1"
fi

# Ensure frontend dist exists (minimal placeholder if not built)
if [ ! -d "web/frontend/dist" ]; then
    echo "📁 Creating minimal frontend placeholder..."
    mkdir -p web/frontend/dist
    cat > web/frontend/dist/index.html << 'EOF'
<!DOCTYPE html>
<html>
<head><title>MCPProxy</title></head>
<body>
<h1>MCPProxy</h1>
<p>Frontend not built. Use CLI or API.</p>
<p>For full Web UI, run: make build</p>
</body>
</html>
EOF
fi

# Get version info
VERSION=$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.1.0-dev")
COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

echo "🔨 Building mcpproxy $VERSION ($COMMIT)..."

LDFLAGS="-X main.version=$VERSION -X main.commit=$COMMIT -X main.date=$BUILD_DATE -X mcpproxy-go/internal/httpapi.buildVersion=$VERSION -s -w"

go build -ldflags "$LDFLAGS" -o mcpproxy ./cmd/mcpproxy

echo "✅ Built: ./mcpproxy"
./mcpproxy --version

if [ "$INSTALL_FLAG" == "--install" ]; then
    echo ""
    echo "📦 Installing to ~/bin/mcpproxy..."
    
    # Backup current version
    if [ -f ~/bin/mcpproxy ]; then
        CURRENT_VERSION=$(~/bin/mcpproxy --version 2>/dev/null | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' || echo "unknown")
        echo "   Backing up current version ($CURRENT_VERSION)..."
        cp ~/bin/mcpproxy ~/bin/mcpproxy.$CURRENT_VERSION.bak 2>/dev/null || true
    fi
    
    cp mcpproxy ~/bin/mcpproxy
    echo "✅ Installed: ~/bin/mcpproxy"
    ~/bin/mcpproxy --version
    
    echo ""
    echo "⚠️  Note: Restart all Cursor windows to use the new version"
fi
