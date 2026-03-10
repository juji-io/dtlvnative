#!/bin/sh

set -eu

ROOT_DIR=$(
  CDPATH= cd -- "$(dirname "$0")/.." && pwd
)
PATCH_FILE="$ROOT_DIR/script/patches/llama-embed-only.patch"
LLAMA_DIR="$ROOT_DIR/src/llama.cpp"

action="${1:-apply}"

case "$action" in
  apply)
    if git -C "$LLAMA_DIR" apply --reverse --check "$PATCH_FILE" >/dev/null 2>&1; then
      exit 0
    fi
    git -C "$LLAMA_DIR" apply "$PATCH_FILE"
    ;;
  revert)
    if git -C "$LLAMA_DIR" apply --reverse --check "$PATCH_FILE" >/dev/null 2>&1; then
      git -C "$LLAMA_DIR" apply --reverse "$PATCH_FILE"
    fi
    ;;
  *)
    echo "usage: $0 apply|revert" >&2
    exit 1
    ;;
esac
