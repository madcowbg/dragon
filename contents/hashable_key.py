from abc import abstractmethod


class HashableKey:
    @property
    @abstractmethod
    def hashed(self) -> bytes: pass
