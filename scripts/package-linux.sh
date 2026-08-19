#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "Usage: $0 VERSION APPIMAGETOOL APPIMAGE_RUNTIME" >&2
    exit 2
fi

version="$1"
appimagetool="$2"
appimage_runtime="$3"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd -- "$script_dir/.." && pwd)"
dist_dir="${DIST_DIR:-$project_dir/dist}"
stage_dir="${PACKAGE_STAGE_DIR:-$project_dir/.linux-package}"
binary="$project_dir/target/release/icedcomm-i2p"
desktop_file="$project_dir/packaging/linux/icedcomm-i2p.desktop"
app_run="$project_dir/packaging/linux/AppRun"
icon="$project_dir/assets/commtools-i2p.png"
readme="$project_dir/README.md"
archive_name="icedcomm-i2p-v${version}-linux-x86_64"
app_dir="$stage_dir/IcedComm-I2P.AppDir"
portable_dir="$stage_dir/$archive_name"

for required in "$binary" "$desktop_file" "$app_run" "$icon" "$readme" "$appimagetool" "$appimage_runtime"; do
    if [[ ! -f "$required" ]]; then
        echo "Required packaging input is missing: $required" >&2
        exit 1
    fi
done

if [[ ! -x "$binary" ]]; then
    echo "Release binary is not executable: $binary" >&2
    exit 1
fi

rm -rf -- "$stage_dir" "$dist_dir"
mkdir -p -- "$dist_dir" "$portable_dir" \
    "$app_dir/usr/bin" \
    "$app_dir/usr/share/applications" \
    "$app_dir/usr/share/icons/hicolor/128x128/apps"

install -m 755 -- "$binary" "$portable_dir/icedcomm-i2p"
install -m 644 -- "$readme" "$portable_dir/README.md"

tar \
    --sort=name \
    --owner=0 \
    --group=0 \
    --numeric-owner \
    --mtime="@${SOURCE_DATE_EPOCH:-0}" \
    -C "$stage_dir" \
    -cf - \
    "$archive_name" \
    | gzip -n -9 > "$dist_dir/$archive_name.tar.gz"

install -m 755 -- "$binary" "$app_dir/usr/bin/icedcomm-i2p"
install -m 755 -- "$app_run" "$app_dir/AppRun"
install -m 644 -- "$desktop_file" "$app_dir/icedcomm-i2p.desktop"
install -m 644 -- "$desktop_file" \
    "$app_dir/usr/share/applications/icedcomm-i2p.desktop"
install -m 644 -- "$icon" "$app_dir/commtools-i2p.png"
install -m 644 -- "$icon" "$app_dir/.DirIcon"
install -m 644 -- "$icon" \
    "$app_dir/usr/share/icons/hicolor/128x128/apps/commtools-i2p.png"

ARCH=x86_64 VERSION="$version" "$appimagetool" \
    --runtime-file "$appimage_runtime" \
    "$app_dir" \
    "$dist_dir/$archive_name.AppImage"
chmod 755 -- "$dist_dir/$archive_name.AppImage"

(
    cd "$dist_dir"
    sha256sum "$archive_name.AppImage" "$archive_name.tar.gz" > SHA256SUMS
)
