"""Environment variable helpers for Kitaru package initialization.

The actual implementation lives in the standalone ``kitaru_init_hook``
package so ZenML can call it via entry points without triggering a
circular ``import kitaru`` -> ``import zenml`` chain. This module
re-exports everything so existing ``from kitaru._env import ...``
imports continue to work.
"""

from kitaru_init_hook import (
    KITARU_ANALYTICS_OPT_IN_ENV,
    KITARU_AUTH_TOKEN_ENV,
    KITARU_DEBUG_ENV,
    KITARU_DEFAULT_ANALYTICS_SOURCE_ENV,
    KITARU_MODEL_REGISTRY_ENV,
    KITARU_PROJECT_ENV,
    KITARU_REPOSITORY_DIRECTORY_NAME,
    KITARU_SERVER_URL_ENV,
    ZENML_ACTIVE_PROJECT_ID_ENV,
    ZENML_ANALYTICS_OPT_IN_ENV,
    ZENML_DEBUG_ENV,
    ZENML_DEFAULT_ANALYTICS_SOURCE_ENV,
    ZENML_STORE_API_KEY_ENV,
    ZENML_STORE_URL_ENV,
    _normalized_kitaru_env,
    _reset_applied,
    apply_env_translations,
)

__all__ = [
    "KITARU_ANALYTICS_OPT_IN_ENV",
    "KITARU_AUTH_TOKEN_ENV",
    "KITARU_DEBUG_ENV",
    "KITARU_DEFAULT_ANALYTICS_SOURCE_ENV",
    "KITARU_MODEL_REGISTRY_ENV",
    "KITARU_PROJECT_ENV",
    "KITARU_REPOSITORY_DIRECTORY_NAME",
    "KITARU_SERVER_URL_ENV",
    "ZENML_ACTIVE_PROJECT_ID_ENV",
    "ZENML_ANALYTICS_OPT_IN_ENV",
    "ZENML_DEBUG_ENV",
    "ZENML_DEFAULT_ANALYTICS_SOURCE_ENV",
    "ZENML_STORE_API_KEY_ENV",
    "ZENML_STORE_URL_ENV",
    "_normalized_kitaru_env",
    "_reset_applied",
    "apply_env_translations",
]
