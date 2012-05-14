from importables import *
from datasets import *
from sync import *
from loaders import *


def chunked_mem_sync(source_loader, destination_loader, sync, hint=16384):
    """A shortcut to do in memory chunked imports.

    >>> from importtools.importables import MockImportable
    >>> from importtools import chunked_mem_sync, full_sync
    >>> source = (MockImportable(id=x, a=x, b=x) for x in range(10))
    >>> destination = (MockImportable(id=x, a=x, b=x) for x in range(5, 15))
    >>> cms = chunked_mem_sync(source, destination, full_sync, 10)
    >>> source, destination = cms.next()
    >>> print destination
    <DiffDataSet: 0 changed, 5 added, 0 removed>
    >>> print sorted(destination.added)
    [<MI a 0, b 0>, <MI a 1, b 1>, <MI a 2, b 2>, <MI a 3, b 3>, <MI a 4, b 4>]

    >>> source, destination = cms.next()
    >>> print destination
    <DiffDataSet: 0 changed, 0 added, 5 removed>
    >>> print sorted(destination.removed)
    ... # doctest: +NORMALIZE_WHITESPACE
    [<MI a 10, b 10>, <MI a 11, b 11>, <MI a 12, b 12>, <MI a 13, b 13>,
     <MI a 14, b 14>]

    >>> source, destination = cms.next()
    Traceback (most recent call last):
        ...
    StopIteration

    """
    l = chunked_loader(source_loader, destination_loader, hint)
    for source, destination in l:
        source_ds = MemoryDataSet(source)
        destination_ds = DiffDataSet(destination)
        sync(source_ds, destination_ds)
        yield source_ds, destination_ds
