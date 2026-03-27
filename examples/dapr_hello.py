from kitaru import checkpoint, flow


@checkpoint
def greet(name: str) -> str:
    return f"hello {name}"


@checkpoint
def better_greet(name: str) -> str:
    return f"hi {name}"


@flow
def my_flow(name: str) -> str:
    first = greet(name)
    result = better_greet(first)
    return result


if __name__ == "__main__":
    handle = my_flow.run(name="world")
    print("exec_id:", handle.exec_id)
    print("result:", handle.wait())
