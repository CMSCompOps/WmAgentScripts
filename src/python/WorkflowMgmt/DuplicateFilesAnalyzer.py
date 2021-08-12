from logging import Logger

from Utilities.Logging import getLogger
from Utilities.DataTools import countLumisPerFile

from typing import Optional


class DuplicateFilesAnalyzer(object):
    """
    _DuplicateFilesAnalyzer_
    General API for analyzing duplicate files
    """

    def __init__(self, logger: Optional[Logger] = None) -> None:
        try:
            super().__init__()
            self.logger = logger or getLogger(self.__class__.__name__)

        except Exception as error:
            raise Exception(f"Error initializing DuplicateFilesAnalyzer\n{str(error)}")

    def _buildGraph(self, filesPerlumis: dict) -> dict:
        """
        The function to build the lumis graph
        :param filesPerlumis: dict of files per lumis
        :return: files graph
        """
        graph = {}

        for _, files in filesPerlumis.items():
            for i, j in [(0, 1), (1, 0)]:
                if files[i] not in graph:
                    graph[files[i]] = {}
                if files[j] not in graph[files[i]]:
                    graph[files[i]][files[j]] = 1

        return graph

    def _hasEdges(self, graph: dict) -> bool:
        """
        The function to check if a given graph has edges
        :param graph: files graph
        :return: True if at least one edge is between two vertices — i. e. there is at least one lumi present in two diferent files, False o/w.
        """
        for v in graph.values():
            if v:
                return True
        return False

    def _deleteByColorBipartiteGraph(self, graph: dict, events: dict) -> list:
        """
        The function to remove duplication by identifying a bipartite graph and removing the smaller side
        :param graph: files graph
        :param events: events by file
        :return: list of files to remove
        """
        red, green = set(), set()

        for fileOne, files in graph.items():
            isFileOneRed = fileOne in red
            isFileOneGreen = fileOne in green

            for fileTwo in files:
                isFileTwoRed = fileTwo in red
                isFileTwoGreen = fileTwo in green

                if not (isFileOneRed or isFileOneGreen or isFileTwoRed or isFileTwoGreen):
                    red.add(fileOne)
                    green.add(fileTwo)

                elif (
                    (isFileOneRed and isFileOneGreen)
                    or (isFileTwoRed and isFileTwoGreen)
                    or (isFileOneRed and isFileTwoRed)
                    or (isFileOneGreen and isFileTwoGreen)
                ):
                    self.logger.critical("Not a bipartite graph, cannot use this algorithm for removing")
                    raise Exception("Not a bipartite graph")

                elif isFileOneRed != isFileTwoRed and isFileOneGreen != isFileTwoGreen:
                    continue

                elif isFileOneRed:
                    green.add(fileTwo)
                elif isFileOneGreen:
                    red.add(fileTwo)
                elif isFileTwoRed:
                    green.add(fileOne)
                elif isFileTwoGreen:
                    red.add(fileOne)

        redEvents = sum([events[file] for file in red])
        greenEvents = sum([events[file] for file in green])

        return list(red) if redEvents < greenEvents else list(green)

    def _deleteSmallestVertexFirst(self, graph: dict, events: dict) -> list:
        """
        The function to remove by deleting files in a greedy fashion. I. e. by removing the smallest files first until there is no more edges on the graph — which means no more lumis in two different files.
        :param graph: files graph
        :param events: events by file
        :return: files to remove
        """
        self.logger.info("Initial files: %s", len(graph))

        files = []
        sortedFiles = sorted(graph.keys(), key=lambda x: events[x])

        while self._hasEdges(graph):
            smallestVertex = sortedFiles.pop()

            for file in graph[smallestVertex]:
                del graph[file][smallestVertex]
            del graph[smallestVertex]

            files.append(smallestVertex)

        return files

    def getFilesWithDuplicateLumis(self, filesPerLumis: dict) -> list:
        """
        The function to get the files to remove because of duplicate lumis
        :param filesPerLumis: dict of files by lumis
        :return: files to remove
        """
        try:
            lumisPerFile = countLumisPerFile(filesPerLumis)

            duplicateLumis = dict((lumis, files) for lumis, files in filesPerLumis.items() if len(files) > 1)
            graph = self._buildGraph(duplicateLumis)

            try:
                return self._deleteByColorBipartiteGraph(graph, lumisPerFile)

            except Exception as error:
                self.logger.error("Failed to get files by color bipartite graph, will try the greedy algorithm")
                self.logger.error(str(error))

            return self._deleteSmallestVertexFirst(graph, lumisPerFile)

        except Exception as error:
            self.logger.error("Failed to get files with duplicate lumis to remove")
            self.logger.error(str(error))
