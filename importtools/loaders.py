import csv
import heapq
import itertools


__all__ = ['args_loader', 'csv_loader', 'chunked_loader']


def args_loader(source, factory, how_many=1):
    """
    A loader that yields values from a flattened list by grouping values
    together and instantiating a factory.  This loader can be used to
    prepopulate a datasets from command line arguments.

    All ``None`` values returned by the factory are skipped.

    This is a short example where each two consecutive values are grouped
    together and used to create tuple instances from a factory function:

    >>> from importtools import args_loader
    >>> args = ['A1', 'A2', 'B1', 'B2', 'C1', 'C2']
    >>> factory = lambda x, y: tuple([x, y])
    >>> al = args_loader(args, factory, 2)
    >>> sorted(al)
    [('A1', 'A2'), ('B1', 'B2'), ('C1', 'C2')]

    """
    assert len(source) % how_many == 0
    # http://docs.python.org/library/itertools.html#recipes
    arg_groups = zip(*[iter(source)] * how_many)
    for arg_group in arg_groups:
        value = factory(*arg_group)
        if value is not None:
            yield value


def csv_loader(source, factory, columns, has_header=True):
    """
    A loader that yields values from a ``CSV`` ``source`` file object by
    calling the ``factory`` with the values specified in the ``columns``.

    .. note:: The columns are ``0`` indexed.

    >>> import StringIO
    >>> from importtools import csv_loader
    >>> source = StringIO.StringIO('''
    ... R1C0,R1C1,R1C2,R1C3
    ... R2C0,R2C1,R2C2,R2C3
    ... R3C0,R3C1,R3C2,R3C3
    ... '''.strip())
    >>> factory = lambda x, y: tuple([x, y,])

    The file is parsed skipping the header:

    >>> csvl = csv_loader(source, factory, columns=[1, 3], has_header=True)
    >>> sorted(csvl)
    [('R2C1', 'R2C3'), ('R3C1', 'R3C3')]

    The ``has_header`` flag can be used to avoid skipping the header:

    >>> source.seek(0)
    >>> csvl = csv_loader(source, factory, columns=[1, 3], has_header=False)
    >>> sorted(csvl)
    [('R1C1', 'R1C3'), ('R2C1', 'R2C3'), ('R3C1', 'R3C3')]

    By default, ``has_header`` flag is set and the number of column can vary:

    >>> source.seek(0)
    >>> factory = lambda x, y, z: tuple([x, y, z])
    >>> csvl = csv_loader(source, factory, columns=[1, 2, 3])
    >>> sorted(csvl)
    [('R2C1', 'R2C2', 'R2C3'), ('R3C1', 'R3C2', 'R3C3')]

    """
    content = csv.reader(source)
    if has_header:
        content.next()
    for line in content:
        params = [line[column] for column in columns]
        value = factory(*params)
        if value is not None:
            yield value


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
        if not (i1_elemens or i2_elemens):
            break
        yield i1_elemens, i2_elemens


def _iter_const(g, const):
    for value in g:
        yield value, const
