# Kitaru frontend declarations

Add one `<kitaru-version>.toml` file before you create a `python/kitaru/v<kitaru-version>` tag.

```toml
schema-version = 1
kitaru-version = "0.22.0rc0"
ui-tag = "kitaru-ui-v0.2.0-rc.1"
```

The Python release workflow downloads `kitaru-ui.tar.gz` and its published checksum from this exact tag. It verifies the checksum and records the observed value in the wheel manifest. A prerelease frontend tag automatically enables prerelease download support.
