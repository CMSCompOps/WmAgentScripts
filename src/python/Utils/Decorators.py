import time
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed

from typing import Callable, Optional, Any, List


def runWithThreads(f: Callable, maxThreads: int = 10) -> list:
    """
    The function that defines a decorator to run a given function with multi threading
    :param f: function
    :param maxThreads: max number of threads
    :return: a list of function outputs
    """

    @wraps(f)
    def wrapper(lst: List[dict]) -> list:
        """
        The function that defines the wrapper to run a given function with multi threading
        :param lst: list of input params
        :return: a list of function outputs
        """
        threadsParam = {"maxThreads": maxThreads}
        result = []
        with ThreadPoolExecutor(
            max_workers=max(threadsParam["maxThreads"], len(lst)),
            thread_name_prefix=f.__name__,
        ) as threadPool:
            threads = {threadPool.submit(f, **kwargs): kwargs for kwargs in lst}
            for thread in as_completed(threads):
                result.append(thread.result())
        return result

    return wrapper
