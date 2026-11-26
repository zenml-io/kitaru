#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Shared credential redaction for MCP output."""

from kitaru.redaction import MASKED as MASKED
from kitaru.redaction import is_credential_name as is_credential_name
from kitaru.redaction import redact as redact
from kitaru.redaction import redact_data as redact_data
