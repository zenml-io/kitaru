#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
"""Model-call cost estimation from the bundled genai-prices catalog."""

from datetime import datetime
from decimal import Decimal
from typing import Any

from genai_prices import Usage, calc_price

from kitaru.api_models.v1.session import TokenUsage

COST_SOURCE = "genai-prices"


def estimate_cost(
    tokens: TokenUsage,
    model: str | None,
    provider: str | None,
    started_at: datetime,
) -> tuple[Decimal | None, dict[str, Any]]:
    """Estimate one model call's USD cost and describe how it was derived.

    Args:
        tokens: Token usage where input tokens include cached input tokens.
        model: Model name to price, or None when it is unknown.
        provider: Provider identifier, or None to let the catalog infer it.
        started_at: Call start time, which selects historical prices.

    Returns:
        The estimated cost, or None when the catalog cannot price the call,
        and node attributes that record the estimate's status and source.
    """
    if model is None:
        return None, {"cost": {"status": "unavailable", "source": COST_SOURCE}}
    try:
        price = calc_price(
            Usage(
                input_tokens=tokens.input_tokens or 0,
                output_tokens=tokens.output_tokens or 0,
                cache_read_tokens=tokens.cached_input_tokens or 0,
            ),
            model,
            provider_id=provider,
            genai_request_timestamp=started_at,
        ).total_price
    except Exception as error:
        return None, {
            "cost": {
                "status": "unavailable",
                "source": COST_SOURCE,
                "error_type": type(error).__name__,
            }
        }
    cost = Decimal(str(price))
    if not cost.is_finite() or cost < 0:
        return None, {"cost": {"status": "unavailable", "source": COST_SOURCE}}
    return cost, {"cost": {"status": "estimated", "source": COST_SOURCE}}
