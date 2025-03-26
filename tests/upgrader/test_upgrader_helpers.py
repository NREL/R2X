from r2x.upgrader.helpers import get_function_arguments


def test_prepare_function_arguments():
    def dummy_function(a, b, c=None):
        pass

    data = {"a": 1, "b": 2, "c": 3, "extra": 4}
    result = get_function_arguments(data, dummy_function)
    expected = {"a": 1, "b": 2, "c": 3}
    assert result == expected, f"Expected {expected}, but got {result}"

    data = {"a": " 1 ", "b": " 2 "}
    result = get_function_arguments(data, dummy_function)
    expected = {"a": 1, "b": 2}
    assert result == expected, f"Expected {expected}, but got {result}"

    data = {"a": 1, "test": {"b": 1, "c": 2}}
    result = get_function_arguments(data, dummy_function)
    expected = {"a": 1, "b": 1, "c": 2}
    assert result == expected, f"Expected {expected}, but got {result}"

    data = {"a": 1, "test": '{"b": 1, "c": 2}'}
    result = get_function_arguments(data, dummy_function)
    expected = {"a": 1, "b": 1, "c": 2}
    assert result == expected, f"Expected {expected}, but got {result}"
