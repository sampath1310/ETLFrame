from abc import abstractmethod, ABC


class IExtractor(ABC):
    @abstractmethod
    def read(self):
        pass


class ITransformer(ABC):
    @abstractmethod
    def transform(self):
        pass


class ILoader(ABC):
    @abstractmethod
    def load(self):
        pass
