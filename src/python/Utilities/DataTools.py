from collections import defaultdict

def countLumisPerFile(filesPerLumis: dict) -> dict:
    """
    The function to count the number of lumis per file
    :param filesPerLumis: dict of files by lumis
    :return: dict of lumis by files
    """
    try:
        lumisPerFile = defaultdict(int)
        for _, files in filesPerLumis.items():
            for file in files:
                lumisPerFile[file] += 1
        return lumisPerFile
    
    except Exception as error:
        print("Failed to count lumis per file")
        print(str(error))
