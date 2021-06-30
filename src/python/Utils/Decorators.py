import time
from functools import wraps

from typing import Callable, Optional, Any


def runWithRetries(
    tries: int = 10, wait: int = 5, default: Optional[Any] = None
) -> Any:
    """
    The function that defines a decorator to run a given function with retries
    :param tries: number of tries
    :param wait: wait time between tries
    :param default: default value to return in case all the tries have failed, raise Exception o/w
    :return: function output, default value o/w
    """

    def decorator(f: Callable):
        @wraps(f)
        def wrapper(*args, **kwargs):
            retriesParam = {"tries": tries, "wait": wait}
            for i in range(retriesParam["tries"]):
                try:
                    return f(*args, **kwargs)

                except Exception as error:
                    print(
                        f"Failed running function {f.__name__} on try #{i+1} of {retriesParam['tries']}"
                    )
                    print(str(error))
                    time.sleep(retriesParam["wait"])

                if default:
                    return default
                raise Exception("NoDefaultValue")

        return wrapper

    return decorator
