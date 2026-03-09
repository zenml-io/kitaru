"""Phase 5 first working Kitaru workflow example.

This is the smallest end-to-end example that exercises the current MVP
primitives: ``@flow`` + ``@checkpoint``.
"""

from kitaru import checkpoint, flow


@checkpoint
def fetch_data(url: str) -> str:
    """Fetch source data.

    Args:
        url: Source URL.

    Returns:
        Mocked source content.
    """
    _ = url
    return "some data"


@checkpoint
def process_data(data: str) -> str:
    """Transform source data.

    Args:
        data: Input data.

    Returns:
        Processed data.
    """
    return data.upper()


@flow
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
    return my_agent.run(url).wait()


def main() -> None:
    """Run the example as a script."""
    result = run_workflow()
    print(result)


if __name__ == "__main__":
    main()
