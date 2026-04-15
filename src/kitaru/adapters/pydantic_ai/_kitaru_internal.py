try:
    from kitaru.runtime import (
        _get_current_checkpoint as get_current_checkpoint,
        _get_current_execution_id as get_current_execution_id,
        _is_inside_checkpoint as is_inside_checkpoint,
        _is_inside_flow as is_inside_flow,
        _suspend_checkpoint_scope as suspend_checkpoint_scope,
    )
except (ImportError, AttributeError) as error:
    raise ImportError(
        'Unsupported Kitaru version for `pydantic_ai.durable_exec.kitaru`: '
        'install a Kitaru release that exposes the `kitaru.runtime` helpers '
        '(`_get_current_checkpoint`, `_get_current_execution_id`, '
        '`_is_inside_checkpoint`, `_is_inside_flow`, `_suspend_checkpoint_scope`).'
    ) from error


__all__ = [
    'get_current_checkpoint',
    'get_current_execution_id',
    'is_inside_checkpoint',
    'is_inside_flow',
    'suspend_checkpoint_scope',
]
