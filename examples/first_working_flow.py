"""Phase 5 first working Kitaru workflow example.

This is the smallest end-to-end example that exercises the current MVP
primitives: ``@kitaru.flow`` + ``@kitaru.checkpoint``.
"""

import kitaru


@kitaru.checkpoint
def fetch_data(url: str) -> str:
    """Fetch source data.

    Args:
        url: Source URL.

    Returns:
        Mocked source content.
    """
    _ = url
    return "some data"


@kitaru.checkpoint
def process_data(data: str) -> str:
    """Transform source data.

    Args:
        data: Input data.

    Returns:
        Processed data.
    """
    return data.upper()


@kitaru.flow
def my_agent(url: str) -> str:
    """Run the example Kitaru workflow.

    Args:
        url: Source URL.

    Returns:
        Processed result.
    """
    data = fetch_data(url)
    return process_data(data)


def run_workflow(url: str = "https://example.com") -> str:
    """Execute the example workflow and return its output.

    Args:
        url: Source URL.

    Returns:
        Workflow output.
    """
    return my_agent(url)


def main() -> None:
    """Run the example as a script."""
    result = run_workflow()
    print(result)


if __name__ == "__main__":
    main()
