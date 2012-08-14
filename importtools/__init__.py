import heapq
import itertools

from importtools.importables import *
from importtools.datasets import *

try:
    from importtools._django import *
except ImportError:
    pass


def chunked_mem_sync(source_loader, destination_loader,
                     DSFactory=RecordingDataSet, hint=16384):
    """A shortcut for chunked imports."""
    l = chunked_loader(source_loader, destination_loader, hint)
    for source, destination in l:
        dest_ds = DSFactory(destination)
        dest_ds.sync(source)
        yield dest_ds


def chunked_loader(ordered_iter1, ordered_iter2, chunk_hint=16384):
    """A loading strategy for running large imports as multiple smaller ones.

    The main functionality of this loader is to split two order iterators in
    smaller lists while keeping the ordering and their combined length around
    ``chunk_hint`` size.

    >>> from importtools import chunked_loader
    >>> loader = chunked_loader([10, 20, 30, 40], [11, 12, 50, 60], 5)
    >>> source, destination = loader.next()
    >>> sorted(source)
    [10, 20, 30]
    >>> sorted(destination)
    [11, 12]
    >>> source, destination = loader.next()
    >>> sorted(source)
    [40]
    >>> sorted(destination)
    [50, 60]
    >>> source, destination = loader.next()
    Traceback (most recent call last):
        ...
    StopIteration

    The algorithm includes the next element in the chunk if it's equal to the
    last one already included:

    >>> loader = chunked_loader([10, 20, 30, 40], [11, 12, 30, 60], 5)
    >>> source, destination = loader.next()
    >>> sorted(source)
    [10, 20, 30]
    >>> sorted(destination)
    [11, 12, 30]

    In case the number of elements its divisible by the chunk size, the results
    should still be correct:

    >>> loader = chunked_loader([10, 20, 30, 40, 50], [11, 12, 13, 60, 70], 5)
    >>> source, destination = loader.next()
    >>> sorted(source)
    [10, 20]
    >>> sorted(destination)
    [11, 12, 13]
    >>> source, destination = loader.next()
    >>> sorted(source)
    [30, 40, 50]
    >>> sorted(destination)
    [60, 70]
    >>> source, destination = loader.next()
    Traceback (most recent call last):
        ...
    StopIteration

    If one of the loader is empty, the algorithm should still function
    correctly:

    >>> loader = chunked_loader([1, 2, 3, 4, 5, 6], [], 5)
    >>> source, destination = loader.next()
    >>> sorted(source)
    [1, 2, 3, 4, 5]
    >>> sorted(destination)
    []
    >>> source, destination = loader.next()
    >>> sorted(source)
    [6]
    >>> sorted(destination)
    []
    >>> loader = chunked_loader([], [1, 2, 3, 4, 5, 6], 5)
    >>> source, destination = loader.next()
    >>> sorted(source)
    []
    >>> sorted(destination)
    [1, 2, 3, 4, 5]
    >>> source, destination = loader.next()
    >>> sorted(source)
    []
    >>> sorted(destination)
    [6]

    """
    i1 = _iter_const(ordered_iter1, True)
    i2 = _iter_const(ordered_iter2, False)
    iterator = heapq.merge(i1, i2)
    while True:
        i1_elemens = list()
        i2_elemens = list()
        current_chunk = itertools.islice(iterator, chunk_hint)
        for element, from_iter1 in current_chunk:
            if from_iter1:
                i1_elemens.append(element)
            else:
                i2_elemens.append(element)
        for next_element, next_from_iter1 in iterator:
            if next_element == element:
                if next_from_iter1:
                    i1_elemens.append(next_element)
                else:
                    i2_elemens.append(next_element)
            else:
                e = (next_element, next_from_iter1)
                iterator = itertools.chain([e], iterator)
                break
        if not i1_elemens and not i2_elemens:
            break
        yield i1_elemens, i2_elemens


def _iter_const(g, const):
    for value in g:
        yield value, const
