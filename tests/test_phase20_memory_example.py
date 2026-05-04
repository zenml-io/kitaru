"""Temporarily skipped until the memory examples cleanup PR.

TODO(PR2 remove-memory examples cleanup): restore this module once the example no
longer imports the removed Kitaru-native memory API.
"""

import pytest

pytest.skip(
    "Kitaru-native memory example cleanup is scheduled for PR2.",
    allow_module_level=True,
)
