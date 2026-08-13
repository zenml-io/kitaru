# Kitaru frontend declarations

Add one `<kitaru-version>.toml` file before you create a `python/kitaru/v<kitaru-version>` tag.

```toml
schema-version = 1
kitaru-version = "0.22.0rc1"
ui-repository = "zenml-io/zenml-frontend-monorepo"
ui-tag = "kitaru-ui-v0.3.0-rc.1"
ui-archive = "kitaru-ui.tar.gz"
ui-sha256 = "<64 lowercase hexadecimal characters>"
allow-prerelease = true
```

The Python release workflow downloads this exact archive and rejects a different checksum. Stable Kitaru versions must select a stable frontend tag and set `allow-prerelease = false`.
