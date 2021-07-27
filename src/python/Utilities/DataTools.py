from collections import defaultdict
from typing import Optional, List, Tuple

from Utilities.IteratorTools import mapValues, filterKeys


def filterRecoveryFilesAndLocations(recoveryDocs: List[dict], suffixTaskFilter: Optional[str] = None) -> dict:
    """
    The function to filter the files and locations for the given recovery docs
    :param recoveryDocs: recovery docs
    :param suffixTaskFilter: filter tasks ending with given suffix
    :return: a dict of files and locations
    """
    filesAndLocations = defaultdict(set)
    for doc in recoveryDocs:
        task = doc.get("fileset_name", "")
        if suffixTaskFilter and not task.endswith(suffixTaskFilter):
            continue

        for filename, data in doc["files"].items():
            filesAndLocations[filename].update(data.get("locations", []))

    filesAndLocations = mapValues(list, filesAndLocations)

    print(f"{len(filesAndLocations)} files in recovery")
    return filesAndLocations


def filterFilesAndLocationsInDBS(filesAndLocations: dict) -> Tuple[dict, dict]:
    """
    The function to filter the files in DBS and the files not in DBS
    :param filesAndLocations: dict of files and locations
    :return: two dicts of files and locations
    """
    filesInDBS, filesNotInDBS = set(), set()
    for filename in filesAndLocations:
        if any(filename.startswith(location) for location in ["/store/unmerged/", "MCFakeFile-"]):
            filesNotInDBS.add(filename)
        else:
            filesInDBS.add(filename)

    inDBS = mapValues(list, filterKeys(filesInDBS, filesAndLocations))
    notInDBS = mapValues(list, filterKeys(filesNotInDBS, filesAndLocations))
    return inDBS, notInDBS


def filterLumisAndFilesByRuns(filesByLumis: dict, lumisByRun: dict, runs: list) -> Tuple[dict, dict]:
    """
    The function to get the lumi sections and files filtered by given runs
    :param filesByLumis: a dict of format {run: [lumis]}
    :param lumisByRun: a dict of format {(run:lumis): [files]}
    :param runs: run numbers
    :return: a dict of format {run: [lumis]} and a dict of format {(run:lumis): [files]}
    """
    return filterKeys(runs, lumisByRun, filesByLumis)


def filterLumisAndFilesByLumis(filesByLumis: dict, lumisByRun: dict, lumis: dict) -> Tuple[dict, dict]:
    """
    The function to get the lumi sections and files filtered by given lumi sections
    :param filesByLumis: a dict of format {run: [lumis]}
    :param lumisByRun: a dict of format {(run:lumis): [files]}
    :param lumis: a dict of format {run: [lumis]}
    :return: a dict of format {run: [lumis]} and a dict of format {(run:lumis): [files]}
    """
    runs = map(int, lumis.keys())
    lumis = set((k, vi) for k, v in lumis.items() for vi in v)
    lumisByRun = filterKeys(runs, lumisByRun)
    filesByLumis = filterKeys(lumis, filesByLumis)
    return lumisByRun, filesByLumis


def flattenTaskTree(task: str, **selectParam) -> list:
    """
    The function to flatten a task tree into a list
    :param task: task
    :param selectParam: optional selection params
    :return: list of tasks
    """
    try:
        allTasks = []
        if selectParam:
            for k, v in selectParam.items():
                if (isinstance(v, list) and getattr(task, k) in v) or (
                    not isinstance(v, list) and getattr(task, k) == v
                ):
                    allTasks.append(task)
                    break
        else:
            allTasks.append(task)

        for child in task.tree.childNames:
            childSpec = getattr(task.tree.children, child)
            allTasks.extend(flattenTaskTree(childSpec, **selectParam))

        return allTasks

    except Exception as error:
        print("Failed to flatten task tree to list")
        print(str(error))
