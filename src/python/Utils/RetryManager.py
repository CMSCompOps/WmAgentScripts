import time

from typing import Callable, Optional, Any


def RunWithRetries(
    fcn: Callable,
    fcnPArgs: list,
    fcnArgs: dict = {},
    default: Optional[Any] = None,
    tries: int = 10,
    wait: int = 5,
) -> Any:
    """
    The function to run a given function with retries
    :param fcn: function
    :param fcnPArgs: function arguments
    :param fcnArgs: optional function arguments
    :param default: default value to return in case all the tries have failed, raise Exception o/w
    :param tries: number of tries
    :param wait: wait time between tries
    :return: function output, default value o/w
    """
    for i in range(tries):
        try:
            return fcn(*fcnPArgs, **fcnArgs)

        except Exception as error:
            print(
                f"Failed to get to run function {fcn.__name__} with arguments {fcnPArgs} and {fcnArgs} on try #{i+1} of {tries}"
            )
            print(str(error))
            time.sleep(wait)

    if default:
        return default
    raise Exception("NoDefaultValue")
