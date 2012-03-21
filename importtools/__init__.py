from importables import *
from datasets import *
from sync import *
from loaders import *


def chunked_mem_sync(source_loader, destination_loader, sync, hint=16384):
    """A shortcut to do in memory chunked imports.

    >>> from importtools import MockImportable, chunked_mem_sync, full_sync
    >>> source = (MockImportable(x) for x in range(10))
    >>> destination = (MockImportable(x) for x in range(5, 15))
    >>> cms = chunked_mem_sync(source, destination, full_sync, 10)
    >>> source, destination = cms.next()
    >>> print destination
    To be added: 5
    <(0,) IMPORTED>
    <(1,) IMPORTED>
    <(2,) IMPORTED>
    <(3,) IMPORTED>
    <(4,) IMPORTED>
    >>> source, destination = cms.next()
    >>> print destination
    To be removed: 5
    <(10,) IMPORTED>
    <(11,) IMPORTED>
    <(12,) IMPORTED>
    <(13,) IMPORTED>
    <(14,) IMPORTED>
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
