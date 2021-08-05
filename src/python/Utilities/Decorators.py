from time import sleep
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed

from typing import Callable, Optional, Any


def runWithRetries(tries: int = 10, wait: int = 5, default: Optional[Any] = None) -> Any:
    """
    The function to define a decorator to run a given function with retries
    :param tries: number of tries
    :param wait: wait time between tries
    :param default: default value to return in case all the tries have failed, raise Exception o/w
    :return: function output, default value o/w
    """

    def decorator(f: Callable) -> Any:
        @wraps(f)
        def wrapper(*args, **kwargs) -> Any:
            params = {"tries": tries, "wait": wait}
            for i in range(params["tries"]):
                try:
                    return f(*args, **kwargs)

                except Exception as error:
                    print(f"Failed running function {f.__name__} on try #{i+1} of {params['tries']}")
                    print(str(error))
                    sleep(params["wait"])

            if default is not None:
                return default
            raise Exception("NoDefaultValue")

        return wrapper

    return decorator


def runWithMultiThreading(mtParam: str, maxThreads: int = 10, timeout: Optional[float] = None, wait: int = 0) -> list:
    """
    The function to define a decorator to run a given function with multi threading
    :param mtParam: multi threaded param
    :param maxThreads: max number of threads
    :param timeout: seconds to stop waiting for result if any, unlimited if None
    :param wait: seconds to wait between results checks
    :return: a list of function outputs
    """

    def decorator(f: Callable) -> list:
        @wraps(f)
        def wrapper(*args, **kwargs) -> list:
            params = {"mtParam": mtParam, "maxThreads": maxThreads, "timeout": timeout, "wait": wait}
            mtThreaded = kwargs.pop(params["mtParam"], [])

            result = []
            with ThreadPoolExecutor(
                max_workers=min(params["maxThreads"], len(mtThreaded)),
                thread_name_prefix=f.__name__,
            ) as threadPool:
                threads = {threadPool.submit(f, *args, **kwargs, **{params["mtParam"]: i}): i for i in mtThreaded}
                for thread in as_completed(threads):
                    threadResult = thread.result(timeout=params["timeout"])
                    if threadResult is None:
                        sleep(params["wait"])
                        continue
                    if isinstance(threadResult, list):
                        result.extend(threadResult)
                    else:
                        result.append(threadResult)
            return result

        return wrapper

    return decorator
