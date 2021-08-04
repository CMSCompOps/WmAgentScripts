from typing import List


def filterSplittingsTaskTypes(splittings: List[dict]) -> List[dict]:
    """
    The function to filter tasks types in splittings schema
    :param splittings: workflow name
    :return: a list of dicts where task types are production, processing or skim
    """
    tasksToKeep = ["Production", "Processing", "Skim"]
    return [splt for splt in splittings if splt["taskType"] in tasksToKeep]


def filterSplittingsParam(splittings: List[dict]) -> List[dict]:
    """
    The function to drop params from splittings schema
    :param splittings: workflow name
    :return: a list of dicts
    """
    paramsToDrop = [
        "algorithm",
        "trustPUSitelists",
        "trustSitelists",
        "deterministicPileup",
        "type",
        "include_parents",
        "lheInputFiles",
        "runWhitelist",
        "runBlacklist",
        "collectionName",
        "group",
        "couchDB",
        "couchURL",
        "owner",
        "initial_lfn_counter",
        "filesetName",
        "runs",
        "lumis",
    ]
    lumiBasedParamsToDrop = ["events_per_job", "job_time_limit"]

    cleanSplittings = []
    for splt in splittings:
        for param in paramsToDrop:
            splt["splitParams"].pop(param, None)

        if splt["splitAlgo"] is "LumiBased":
            for param in lumiBasedParamsToDrop:
                splt["splitParams"].pop(param, None)
        cleanSplittings.append(splt)
    return cleanSplittings
