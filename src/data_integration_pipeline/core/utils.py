from typing import Iterable, Any
from itertools import islice


def batch_yielder(initial_yielder: Iterable[Any], batch_size: int) -> Iterable[list[Any]]:
    """Yield successive n-sized chunks from an iterable using islice."""
    it = iter(initial_yielder)
    while True:
        # creates a 'slice' of the iterator of length batch_size
        chunk = list(islice(it, batch_size))
        if not chunk:
            break
        yield chunk
