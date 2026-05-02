# Local MCPProxy Build

This is a local checkout of [mcpproxy-go](https://github.com/smart-mcp-proxy/mcpproxy-go) for building custom versions.

## Why Local Build?

- Control over which version is installed
- Shared across all Cursor workspaces (each workspace runs its own daemon from `~/bin/mcpproxy`)
- No dependency on Homebrew for updates

## Quick Commands

```bash
# Update to latest version, build, and install
./build-local.sh --update

# Build only (creates ./mcpproxy in this directory)
./build-local.sh

# Build and install to ~/bin
./build-local.sh --install
```

## After Installing

**Restart ALL Cursor windows** - each workspace runs its own mcpproxy daemon that needs to pick up the new binary.

## Building a Specific Version

```bash
git fetch --tags
git tag --sort=-v:refname | head -10  # List recent versions
git checkout v0.16.3                   # Checkout specific version
./build-local.sh --install
```

## Requirements

- **Go 1.21+**: Install via `mise use -g go@latest`
- The build script creates a minimal frontend placeholder (full Web UI requires `make build` with Node.js)

## Minimum Version Requirements

| Feature | Minimum Version |
|---------|-----------------|
| OAuth callback port persistence | v0.14.0+ |
| RFC 8414 OAuth discovery | v0.15.0+ |

## Related Documentation

- eng_ai_assistant: `.cursor/rules/mcp-auth.mdc` - Authentication troubleshooting
- Upstream repo: https://github.com/smart-mcp-proxy/mcpproxy-go
