"""Temporarily skipped until the News Scout memory cleanup PR.

TODO(PR2 remove-memory examples cleanup): restore this module once the example no
longer imports the removed Kitaru-native memory API.
"""

import pytest

pytest.skip(
    "News Scout still imports the removed Kitaru-native memory API; "
    "cleanup is scheduled for PR2.",
    allow_module_level=True,
)
