#!/bin/bash
# Raise the file descriptor limit for the server process.
# This matches ZenML's server entrypoint behavior.
ulimit -n 65535
exec "$@"
