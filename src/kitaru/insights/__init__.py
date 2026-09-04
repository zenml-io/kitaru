#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Post-import insight generation."""

from kitaru.insights.generation import (
    InsightModelGenerator,
    ModelGenerationConfig,
)
from kitaru.insights.models import (
    INSIGHT_METADATA_KEY,
    Coverage,
    CoverageTruncation,
    EvidenceLocator,
    GenerationDiagnostics,
    GenerationMode,
    GenerationVersions,
    InsightCardMetadata,
    InsightGenerationContext,
    InsightGenerationResult,
    PageIntro,
    PageRecommendation,
    ProviderReceipt,
    SourceImportContext,
)
from kitaru.insights.observability import GenerationObserver
from kitaru.insights.pipeline import (
    InsightGenerationConfig,
    InsightResultSizeError,
    generate_insights,
)

__all__ = [
    "INSIGHT_METADATA_KEY",
    "Coverage",
    "CoverageTruncation",
    "EvidenceLocator",
    "GenerationDiagnostics",
    "GenerationMode",
    "GenerationObserver",
    "GenerationVersions",
    "InsightCardMetadata",
    "InsightGenerationConfig",
    "InsightGenerationContext",
    "InsightGenerationResult",
    "InsightModelGenerator",
    "InsightResultSizeError",
    "ModelGenerationConfig",
    "PageIntro",
    "PageRecommendation",
    "ProviderReceipt",
    "SourceImportContext",
    "generate_insights",
]
