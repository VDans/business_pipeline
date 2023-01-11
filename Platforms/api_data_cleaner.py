from abc import ABC, abstractmethod


class ApiDataCleaner(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def map_columns(self):
        pass


class RentlioAPI(ApiDataCleaner):
    def map_columns(self):
        pass
