import heapq
import itertools


__all__ = [
    'full_sync', 'add_sync', 'pop_sync', 'update_sync', 'chunked_loader'
]


def full_sync(source, destination):
    """
    An import strategy that adds the new elements from source in destination
    and deletes from the destination the elements that are not present in the
    source anymore.  The import algorithm uses a
    :py:class:`~.importtools.datasets.RODataSet` implementation as the data
    source and a :py:class:`~.importtools.datasets.DataSet` implementation as
    the destination.

    The algorithm calls the
    :py:meth:`~.importtools.importables.Importable.update` method.

    In order to see the import in action we need to create a source and a
    destination, for the simplicity sake two
    :py:class:`~.importtools.datasets.MemoryDataSet` instances are used:

    >>> from importtools import MemoryDataSet
    >>> destination = MemoryDataSet()
    >>> source = MemoryDataSet()

    The destination and the source can now be populated with elements and the
    import should work like expected:

    >>> from importtools.importables import MockImportable
    >>> from importtools import Importable, full_sync

    >>> destination.add(MockImportable(id=0, a=0, b=0))
    >>> destination.add(MockImportable(id=1, a=1, b=1))
    >>> source.add(MockImportable(id=2, a=2, b=2))
    >>> source.add(MockImportable(id=3, a=3, b=3))
    >>> full_sync(source, destination)
    >>> sorted(destination)
    [<MI a 2, b 2>, <MI a 3, b 3>]

    In case the source contains a updated elements the destination is updated
    to reflect the differences:

    >>> source = MemoryDataSet()
    >>> source.add(MockImportable(id=0, a=100, b=100))
    >>> destination = MemoryDataSet()
    >>> destination.add(MockImportable(id=0, a=0, b=0))
    >>> full_sync(source, destination)
    >>> sorted(destination)
    [<MI a 100, b 100>]

    """
    source_set = set(source)
    destination_set = set(destination)
    _update_sync(source, destination, source_set & destination_set)
    _pop_sync(destination, destination_set - source_set)
    _add_sync(destination, source_set - destination_set)


def add_sync(source, destination):
    source_set = set(source)
    destination_set = set(destination)
    _add_sync(destination, source_set - destination_set)


def _add_sync(destination, s):
    map(destination.add, s)


def pop_sync(source, destination):
    source_set = set(source)
    destination_set = set(destination)
    _pop_sync(destination, destination_set - source_set)


def _pop_sync(destination, s):
    map(destination.pop, s)


def update_sync(source, destination):
    source_set = set(source)
    destination_set = set(destination)
    _update_sync(source, destination, source_set & destination_set)


def _update_sync(source, destination, s):
    for element in s:
        s_element = source.get(element)
        d_element = destination.get(element)
        d_element.update(s_element)


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
