from abc import ABC, abstractmethod

from storage.tuple import Tuple


class Executor(ABC):
    @abstractmethod
    def init(self) -> None: ...

    @abstractmethod
    def next(self) -> Tuple | None: ...
