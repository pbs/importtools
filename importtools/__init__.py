from importables import *
from datasets import *
from sync import *
from loaders import *


def chunked_mem_sync(source_loader, destination_loader, sync, hint=16384):
    """A shortcut to do in memory chunked imports. """
    l = chunked_loader(source_loader, destination_loader, hint)
    for source, destination in l:
        source_ds = MemoryDataSet(source)
        destination_ds = DiffDataSet(destination)
        sync(source_ds, destination_ds)
        yield source_ds, destination_ds
