from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed

from typing import Callable, Optional, Any, List


def runWithMultiThreading(f: Callable, maxThreads: int = 10) -> list:
    """
    The function that defines a decorator to run a given function with multi threading
    :param f: function
    :param maxThreads: max number of threads
    :return: a list of function outputs
    """

    @wraps(f)
    def wrapper(self, lst: List[dict]) -> list:
        """
        The function that defines the wrapper to run a given function with multi threading
        :param lst: list of input params
        :return: a list of function outputs
        """
        threadsParam = {"maxThreads": maxThreads}
        result = []
        with ThreadPoolExecutor(
            max_workers=min(threadsParam["maxThreads"], len(lst)),
            thread_name_prefix=f.__name__,
        ) as threadPool:
            threads = {threadPool.submit(f, self, **kwargs): kwargs for kwargs in lst}
            for thread in as_completed(threads):
                threadResult = thread.result()
                if threadResult is None:
                    continue
                if isinstance(threadResult, list):
                    result.extend(threadResult)
                else:
                    result.append(threadResult)
        return result

    return wrapper
