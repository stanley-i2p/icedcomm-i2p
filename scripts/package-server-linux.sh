#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 VERSION" >&2
    exit 2
fi

version="$1"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd -- "$script_dir/.." && pwd)"
dist_dir="${DIST_DIR:-$project_dir/dist}"
stage_dir="${SERVER_PACKAGE_STAGE_DIR:-$project_dir/.server-linux-package}"
binary="$project_dir/SERVER/target/release/deaddrop-server"
readme="$project_dir/SERVER/README.md"
license="$project_dir/LICENSE"
notice="$project_dir/NOTICE"
archive_name="deaddrop-server-v${version}-linux-x86_64"
portable_dir="$stage_dir/$archive_name"

for required in "$binary" "$readme" "$license" "$notice"; do
    if [[ ! -f "$required" ]]; then
        echo "Required server packaging input is missing: $required" >&2
        exit 1
    fi
done

if [[ ! -x "$binary" ]]; then
    echo "Server release binary is not executable: $binary" >&2
    exit 1
fi

rm -rf -- "$stage_dir"
mkdir -p -- "$dist_dir" "$portable_dir"

install -m 755 -- "$binary" "$portable_dir/deaddrop-server"
install -m 644 -- "$readme" "$portable_dir/README.md"
install -m 644 -- "$license" "$portable_dir/LICENSE"
install -m 644 -- "$notice" "$portable_dir/NOTICE"

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
