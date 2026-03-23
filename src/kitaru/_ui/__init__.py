# Bundled Kitaru UI assets for local-server dashboard replacement.
#
# At build/release time, scripts/download-ui.sh populates:
#   _ui/dist/        — extracted Kitaru UI static files
#   _ui/bundle_manifest.json — version + SHA256 identity
#
# At runtime, _local_server.py reads these to patch ZenML's
# installed dashboard directory before starting the local daemon.
