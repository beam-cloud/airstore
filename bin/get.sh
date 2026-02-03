#!/bin/sh
set -e

# Airstore CLI Installer
# Usage: curl -fsSL https://get.airstore.ai | sh

REPO="beam-cloud/airstore"
BINARY_NAME="airstore"
INSTALL_DIR="/usr/local/bin"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

info() {
    printf "${GREEN}==>${NC} %s\n" "$1"
}

warn() {
    printf "${YELLOW}Warning:${NC} %s\n" "$1"
}

error() {
    printf "${RED}Error:${NC} %s\n" "$1" >&2
    exit 1
}

prompt() {
    printf "${BLUE}==>${NC} %s " "$1"
}

# Check if Homebrew is installed
check_homebrew() {
    if command -v brew >/dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Install Homebrew
install_homebrew() {
    info "Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    
    # Add Homebrew to PATH for this session
    if [ -f "/opt/homebrew/bin/brew" ]; then
        eval "$(/opt/homebrew/bin/brew shellenv)"
    elif [ -f "/usr/local/bin/brew" ]; then
        eval "$(/usr/local/bin/brew shellenv)"
    fi
}

# Install fuse-t on macOS
install_fuse_t() {
    OS=$(detect_os)
    
    if [ "$OS" != "darwin" ]; then
        info "Skipping fuse-t installation (not macOS)"
        return 0
    fi
    
    info "Checking for fuse-t (required for mounting on macOS)..."
    
    # Check if fuse-t is already installed
    if [ -d "/Library/Application Support/fuse-t" ] || brew list --cask fuse-t >/dev/null 2>&1; then
        info "fuse-t is already installed"
        return 0
    fi
    
    # Check for Homebrew
    if ! check_homebrew; then
        warn "Homebrew is not installed."
        printf "Would you like to install Homebrew? [y/N] "
        read -r response
        case "$response" in
            [yY][eE][sS]|[yY])
                install_homebrew
                ;;
            *)
                warn "Skipping fuse-t installation (Homebrew required)"
                warn "You can install it later with: brew install --cask macfuse"
                return 0
                ;;
        esac
    fi
    
    # Install fuse-t
    info "Installing fuse-t..."
    brew tap macos-fuse-t/homebrew-cask
    brew install --cask fuse-t
    
    info "fuse-t installed successfully"
}

# Detect OS
detect_os() {
    case "$(uname -s)" in
        Linux*)  echo "linux" ;;
        Darwin*) echo "darwin" ;;
        *)       error "Unsupported operating system: $(uname -s)" ;;
    esac
}

# Detect architecture
detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64)  echo "amd64" ;;
        arm64|aarch64) echo "arm64" ;;
        *)             error "Unsupported architecture: $(uname -m)" ;;
    esac
}

# Get latest version from GitHub
get_latest_version() {
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/'
    elif command -v wget >/dev/null 2>&1; then
        wget -qO- "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/'
    else
        error "Neither curl nor wget found. Please install one of them."
    fi
}

# Download file
download() {
    url="$1"
    output="$2"
    
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$url" -o "$output"
    elif command -v wget >/dev/null 2>&1; then
        wget -q "$url" -O "$output"
    else
        error "Neither curl nor wget found. Please install one of them."
    fi
}

main() {
    OS=$(detect_os)
    ARCH=$(detect_arch)
    
    info "Detected OS: ${OS}, Architecture: ${ARCH}"
    
    # Get latest version
    info "Fetching latest version..."
    VERSION=$(get_latest_version)
    
    if [ -z "$VERSION" ]; then
        error "Failed to get latest version"
    fi
    
    # Remove 'v' prefix if present for filename
    VERSION_NUM="${VERSION#v}"
    
    info "Latest version: ${VERSION}"
    
    # Construct download URL
    FILENAME="${BINARY_NAME}-${VERSION_NUM}-${OS}-${ARCH}.tar.gz"
    DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${FILENAME}"
    
    # Create temp directory
    TMP_DIR=$(mktemp -d)
    trap "rm -rf ${TMP_DIR}" EXIT
    
    info "Downloading ${FILENAME}..."
    download "$DOWNLOAD_URL" "${TMP_DIR}/${FILENAME}"
    
    # Extract
    info "Extracting..."
    tar -xzf "${TMP_DIR}/${FILENAME}" -C "${TMP_DIR}"
    
    # Find the binary (it might be in a subdirectory or at root)
    if [ -f "${TMP_DIR}/${BINARY_NAME}" ]; then
        BINARY_PATH="${TMP_DIR}/${BINARY_NAME}"
    elif [ -f "${TMP_DIR}/${BINARY_NAME}-${VERSION_NUM}-${OS}-${ARCH}/${BINARY_NAME}" ]; then
        BINARY_PATH="${TMP_DIR}/${BINARY_NAME}-${VERSION_NUM}-${OS}-${ARCH}/${BINARY_NAME}"
    else
        # Try to find it
        BINARY_PATH=$(find "${TMP_DIR}" -name "${BINARY_NAME}" -type f | head -1)
        if [ -z "$BINARY_PATH" ]; then
            error "Could not find ${BINARY_NAME} binary in archive"
        fi
    fi
    
    # Install
    info "Installing to ${INSTALL_DIR}..."
    
    if [ -w "$INSTALL_DIR" ]; then
        mv "$BINARY_PATH" "${INSTALL_DIR}/${BINARY_NAME}"
        chmod +x "${INSTALL_DIR}/${BINARY_NAME}"
    else
        warn "Permission denied for ${INSTALL_DIR}. Using sudo..."
        sudo mv "$BINARY_PATH" "${INSTALL_DIR}/${BINARY_NAME}"
        sudo chmod +x "${INSTALL_DIR}/${BINARY_NAME}"
    fi
    
    # Verify installation
    if command -v "$BINARY_NAME" >/dev/null 2>&1; then
        info "Successfully installed ${BINARY_NAME} ${VERSION}"
    else
        warn "Installation complete, but ${BINARY_NAME} not found in PATH"
        warn "You may need to add ${INSTALL_DIR} to your PATH"
    fi
    
    # Install fuse-t on macOS
    install_fuse_t
    
    echo ""
    info "Installation complete!"
    info "Run '${BINARY_NAME} --help' to get started"
}

main "$@"
