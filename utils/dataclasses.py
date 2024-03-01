from dataclasses import asdict
from collections.abc import Mapping


def iterate_as_dict(cls):
    """Decorator for dataclasses to allow iterating on the class like a dictionary."""

    class DictIterableDataclass(cls, Mapping):
        def __iter__(self): 
            return iter(asdict(self))

        def __getitem__(self, key): 
            return asdict(self)[key]

        def __len__(self): 
            return len(asdict(self))

    return DictIterableDataclass