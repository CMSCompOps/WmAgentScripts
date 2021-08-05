from typing import Callable


def filterKeys(lst: list, data: dict, *otherData: dict) -> dict:
    """
    The function to filter dict data by a given list of keys to keep
    :param lst: key values to keep
    :param data/otherData: dicts
    :return: filtered data (keep the input order if more than one dict is given)
    """
    filteredData = []
    for d in [data] + list(otherData):
        filteredData.append(dict((k, v) for k, v in d.items() if k in lst or (isinstance(k, tuple) and k[0] in lst)))
    return tuple(filteredData) if len(filteredData) > 1 else filteredData[0]


def mapValues(f: Callable, data: dict) -> dict:
    """
    The function to map the values of a dict by a given function
    :param f: the function to apply to values
    :param data: dict
    :return: dict of format {k: f(v)}
    """
    return dict((k, f(v)) for k, v in data.items())


def mapKeys(f: Callable, data: dict) -> dict:
    """
    The function to map the keys of a dict by a given function
    :param f: the function to apply to keys
    :param data: dict
    :return: dict of format {f(k): v}
    """
    return dict((f(k), v) for k, v in data.items())


def sortByKeys(data: dict) -> dict:
    """
    The function to sort a dictionary by its keys
    :param data: dict
    :return: sorted dict
    """
    return dict(sorted(data.items(), key=lambda item: item[0]))
