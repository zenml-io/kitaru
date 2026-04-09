# Bundled Kitaru UI assets for local-server dashboard replacement.
#
# At build/release time, scripts/download-ui.sh populates:
#   _ui/dist/        — extracted Kitaru UI static files
#   _ui/bundle_manifest.json — version + SHA256 identity
#
# At runtime, _local_server.py sets ZENML_SERVER_DASHBOARD_FILES_PATH
# to point the local server at these assets instead of the default
# ZenML dashboard.
