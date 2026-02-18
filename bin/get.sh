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

# Run a command as root (directly when already root, otherwise via sudo).
run_as_root() {
    if [ "$(id -u)" -eq 0 ]; then
        "$@"
    elif command -v sudo >/dev/null 2>&1; then
        sudo "$@"
    else
        error "Root privileges required to run: $* (install sudo or run as root)"
    fi
}

can_elevate() {
    if [ "$(id -u)" -eq 0 ]; then
        return 0
    fi
    command -v sudo >/dev/null 2>&1
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

# Detect whether we're running in a container runtime.
is_container_runtime() {
    if [ -f "/.dockerenv" ]; then
        return 0
    fi
    if [ -r "/proc/1/cgroup" ] && grep -Eq "(docker|containerd|kubepods|podman|lxc)" /proc/1/cgroup; then
        return 0
    fi
    return 1
}

# Probe whether mount(2) is allowed in this environment.
can_mount_tmpfs() {
    if ! command -v mount >/dev/null 2>&1 || ! command -v umount >/dev/null 2>&1; then
        return 1
    fi

    probe_dir=$(mktemp -d 2>/dev/null || echo "/tmp/airstore-mount-probe-$$")
    mkdir -p "$probe_dir"

    if mount -t tmpfs tmpfs "$probe_dir" >/dev/null 2>&1; then
        umount "$probe_dir" >/dev/null 2>&1 || true
        rmdir "$probe_dir" >/dev/null 2>&1 || true
        return 0
    fi

    rmdir "$probe_dir" >/dev/null 2>&1 || true
    return 1
}

# Enable rootless FUSE allow_other when possible.
enable_fuse_user_allow_other() {
    OS=$(detect_os)
    if [ "$OS" != "linux" ]; then
        return 0
    fi
    if [ ! -f "/etc/fuse.conf" ]; then
        return 0
    fi
    if grep -Eq '^[[:space:]]*user_allow_other([[:space:]]|$)' /etc/fuse.conf; then
        return 0
    fi

    info "Enabling user_allow_other in /etc/fuse.conf for rootless FUSE mounts..."
    run_as_root sh -c "printf '\nuser_allow_other\n' >> /etc/fuse.conf" || \
        warn "Could not update /etc/fuse.conf; rootless FUSE may require manual user_allow_other"
}

# Try to make /dev/fuse available on Linux hosts where the FUSE kernel module
# exists but is not loaded/device node missing (common on fresh VMs).
ensure_fuse_device_linux() {
    OS=$(detect_os)
    if [ "$OS" != "linux" ]; then
        return 0
    fi
    if [ -e "/dev/fuse" ]; then
        return 0
    fi

    warn "/dev/fuse is missing; attempting to enable FUSE kernel support..."

    if ! can_elevate; then
        warn "No root/sudo access to load kernel modules. FUSE will remain unavailable."
        return 0
    fi

    if command -v modprobe >/dev/null 2>&1; then
        if ! run_as_root modprobe fuse >/dev/null 2>&1; then
            warn "modprobe fuse failed (kernel may not expose FUSE module)"
        fi
    fi

    if [ ! -e "/dev/fuse" ] && [ -e "/sys/module/fuse" ] && command -v mknod >/dev/null 2>&1; then
        if ! run_as_root mknod /dev/fuse c 10 229 >/dev/null 2>&1; then
            warn "could not create /dev/fuse device node"
        fi
    fi

    if [ -e "/dev/fuse" ]; then
        run_as_root chmod 666 /dev/fuse >/dev/null 2>&1 || true
        info "/dev/fuse is now available"
    else
        warn "/dev/fuse is still unavailable; Linux will use NFS fallback backend"
    fi
}

# Install Linux mount prerequisites for both FUSE and NFS backends.
install_linux_mount_prereqs() {
    OS=$(detect_os)
    
    if [ "$OS" != "linux" ]; then
        return 0
    fi

    need_nfs_utils=0
    need_fuse_userspace=0

    if ! command -v mount.nfs >/dev/null 2>&1 && ! command -v mount.nfs4 >/dev/null 2>&1; then
        need_nfs_utils=1
    fi

    if ! command -v fusermount >/dev/null 2>&1 && ! command -v fusermount3 >/dev/null 2>&1; then
        need_fuse_userspace=1
    fi

    if [ "$need_nfs_utils" -eq 0 ] && [ "$need_fuse_userspace" -eq 0 ]; then
        info "Linux mount prerequisites are already installed"
    else
        warn "Some Linux mount prerequisites are missing; installing required packages..."
    fi

    if command -v apt-get >/dev/null 2>&1; then
        pkgs=""
        [ "$need_nfs_utils" -eq 1 ] && pkgs="${pkgs} nfs-common"
        [ "$need_fuse_userspace" -eq 1 ] && pkgs="${pkgs} fuse libfuse2"
        if [ -n "$pkgs" ]; then
            info "Installing:${pkgs}"
            run_as_root apt-get update -qq
            if ! run_as_root apt-get install -y -qq $pkgs; then
                if [ "$need_fuse_userspace" -eq 1 ]; then
                    warn "fuse package unavailable; retrying with fuse3 + libfuse2"
                    pkgs_fallback=""
                    [ "$need_nfs_utils" -eq 1 ] && pkgs_fallback="${pkgs_fallback} nfs-common"
                    pkgs_fallback="${pkgs_fallback} fuse3 libfuse2"
                    run_as_root apt-get install -y -qq $pkgs_fallback || warn "Package install failed; please install prerequisites manually"
                else
                    warn "Package install failed; please install prerequisites manually"
                fi
            fi
        fi
    elif command -v dnf >/dev/null 2>&1; then
        pkgs=""
        [ "$need_nfs_utils" -eq 1 ] && pkgs="${pkgs} nfs-utils"
        [ "$need_fuse_userspace" -eq 1 ] && pkgs="${pkgs} fuse fuse-libs"
        if [ -n "$pkgs" ]; then
            info "Installing:${pkgs}"
            if ! run_as_root dnf install -y $pkgs; then
                if [ "$need_fuse_userspace" -eq 1 ]; then
                    warn "fuse3 package unavailable; retrying with fuse"
                    pkgs_fallback=""
                    [ "$need_nfs_utils" -eq 1 ] && pkgs_fallback="${pkgs_fallback} nfs-utils"
                    pkgs_fallback="${pkgs_fallback} fuse3"
                    run_as_root dnf install -y $pkgs_fallback || warn "Package install failed; please install prerequisites manually"
                else
                    warn "Package install failed; please install prerequisites manually"
                fi
            fi
        fi
    elif command -v yum >/dev/null 2>&1; then
        pkgs=""
        [ "$need_nfs_utils" -eq 1 ] && pkgs="${pkgs} nfs-utils"
        [ "$need_fuse_userspace" -eq 1 ] && pkgs="${pkgs} fuse fuse-libs"
        if [ -n "$pkgs" ]; then
            info "Installing:${pkgs}"
            run_as_root yum install -y $pkgs || warn "Package install failed; please install prerequisites manually"
        fi
    elif command -v apk >/dev/null 2>&1; then
        pkgs=""
        [ "$need_nfs_utils" -eq 1 ] && pkgs="${pkgs} nfs-utils"
        [ "$need_fuse_userspace" -eq 1 ] && pkgs="${pkgs} fuse3"
        if [ -n "$pkgs" ]; then
            info "Installing:${pkgs}"
            run_as_root apk add --no-cache $pkgs || warn "Package install failed; please install prerequisites manually"
        fi
    else
        warn "Could not auto-install Linux mount prerequisites. Please install manually:"
        warn "  Debian/Ubuntu: sudo apt install nfs-common fuse libfuse2"
        warn "  RHEL/Fedora:   sudo dnf install nfs-utils fuse fuse-libs"
        warn "  Alpine:         sudo apk add nfs-utils fuse3"
    fi

    ensure_fuse_device_linux

    if command -v fusermount >/dev/null 2>&1 || command -v fusermount3 >/dev/null 2>&1; then
        enable_fuse_user_allow_other
    fi

    # Re-check and report environment constraints that package install cannot fix.
    if [ ! -e /dev/fuse ]; then
        warn "/dev/fuse is not present. FUSE backend is unavailable; airstore will use NFS fallback."
    fi
    if is_container_runtime && ! can_mount_tmpfs; then
        warn "This container appears to block mount(2) (common in unprivileged containers)."
        warn "Kernel mounts (NFS fallback) and often FUSE mounts may fail without extra capabilities."
        warn "For real mount semantics, run with e.g. '--cap-add SYS_ADMIN --device /dev/fuse --security-opt apparmor=unconfined'."
        warn "Some sandboxes (including many gVisor setups) may disallow mounts entirely."
    fi
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

# Get latest CLI version from GitHub (excludes SDK releases)
get_latest_version() {
    # We need to filter releases to only get CLI versions (v*), not SDK versions (sdk-v*)
    # The /releases/latest endpoint doesn't support filtering, so we query the list
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "https://api.github.com/repos/${REPO}/releases?per_page=20" | \
            grep '"tag_name":' | \
            sed -E 's/.*"([^"]+)".*/\1/' | \
            grep -E '^v[0-9]' | \
            head -1
    elif command -v wget >/dev/null 2>&1; then
        wget -qO- "https://api.github.com/repos/${REPO}/releases?per_page=20" | \
            grep '"tag_name":' | \
            sed -E 's/.*"([^"]+)".*/\1/' | \
            grep -E '^v[0-9]' | \
            head -1
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
        warn "Permission denied for ${INSTALL_DIR}. Using elevated privileges..."
        run_as_root mv "$BINARY_PATH" "${INSTALL_DIR}/${BINARY_NAME}"
        run_as_root chmod +x "${INSTALL_DIR}/${BINARY_NAME}"
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

    # Install Linux mount prerequisites (FUSE + NFS userspace tools)
    install_linux_mount_prereqs
    
    echo ""
    info "Installation complete!"
    info "Run '${BINARY_NAME} --help' to get started"
}

main "$@"
