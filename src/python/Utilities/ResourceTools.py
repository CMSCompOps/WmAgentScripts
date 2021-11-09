import resource

def getMemoryUsage() -> float:
    """
    The function to get the current memory usage
    :return: memory
    """
    try:
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.
    
    except Exception as error:
        print("Failed to compute memory usage")
        print(str(error))