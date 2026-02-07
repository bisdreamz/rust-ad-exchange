#!/bin/sh
# Wrapper to execute binaries under sudo. Set REX_DISABLE_SUDO=1 to skip.

if [ "${REX_DISABLE_SUDO:-0}" = "1" ]; then
    exec "$@"
fi

if command -v sudo >/dev/null 2>&1; then
    exec sudo -E "$@"
fi

exec "$@"
