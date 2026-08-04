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
"""API server process entrypoint."""

import logging

import uvicorn
from fastapi import FastAPI

from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings


def app() -> FastAPI:
    """Create the API application from environment settings.

    Returns:
        Configured FastAPI application.
    """
    settings = APISettings()
    logging.basicConfig(level=settings.LOG_LEVEL)
    return create_app(settings)


def main() -> None:
    """Run the API server."""
    settings = APISettings()
    uvicorn.run(
        app(),
        host=settings.HOST,
        port=settings.PORT,
        log_level=settings.LOG_LEVEL.lower(),
    )


if __name__ == "__main__":
    main()
