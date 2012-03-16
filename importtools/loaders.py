import csv
import abc
import heapq
import itertools


class Loader(object):
    """
    An :py:mod:`abc` that represents a data source used to generate
    :py:class:`~.importtools.importables.Importable` instances.

    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __iter__(self):
        pass


class ArgsLoader(Loader):
    """
    A :py:class:`Loader` implementation that gets prepopulated from a
    flattened list by grouping values together and instantiating a factory.
    The primary use of this implementation is to create a datasets from command
    line arguments.

    This is a short example where each two consecutive values are grouped
    together and used to create tuple instances from a factory function.

    >>> from importtools.loaders import ArgsLoader
    >>> from importtools.datasets import MemoryDataSet
    >>> al = ArgsLoader(lambda x, y: tuple([x, y]), 2)
    >>> al = al.bind(['STATE1', 'CL1', 'STATE2', 'CL2'])
    >>> argsds = MemoryDataSet(al)
    >>> argsds.get(('STATE1', 'CL1'))
    ('STATE1', 'CL1')
    >>> sorted(argsds)
    [('STATE1', 'CL1'), ('STATE2', 'CL2')]

    """
    def __init__(self, factory, how_many=1):
        self.factory = factory
        self.how_many = how_many
        self.source = []

    def bind(self, source):
        """Bind a data source to this loader."""
        self.source = source
        return self

    def _iter(self):
        """Yield instances returned by the factory.

        The factory is called with ``source`` values grouped together as
        dictated by the value of ``how_many`` property.

        """
        source = self.source
        assert len(source) % self.how_many == 0
        # http://docs.python.org/library/itertools.html#recipes
        arg_groups = zip(*[iter(source)] * self.how_many)
        for arg_group in arg_groups:
            yield self.factory(*arg_group)

    def __iter__(self):
        return self._iter()


class CSVLoader(Loader):
    """
    A :py:class:`Loader` implementation that gets prepopulated from a ``CSV``
    file and instantiate :py:class:`~.importtools.importables.Importable`
    instances.

    >>> import StringIO
    >>> source = StringIO.StringIO('''
    ... R1C0,R1C1,R1C2,R1C3
    ... R2C0,R2C1,R2C2,R2C3
    ... R3C0,R3C1,R3C2,R3C3
    ... '''.strip())
    >>> from importtools.datasets import MemoryDataSet

    The file is parsed skipping the header and the ``DataSet`` is populated
    with the correct values:

    >>> source.seek(0)
    >>> csvl = CSVLoader(
    ...     lambda x, y: tuple([x, y]),
    ...     columns=[1,3],
    ...     has_header=True)
    >>> csvl = csvl.bind(source)
    >>> csvds = MemoryDataSet(csvl)
    >>> csvds.get((('R2C1', 'R2C3')))
    ('R2C1', 'R2C3')
    >>> csvds.get((('R3C1', 'R3C3')))
    ('R3C1', 'R3C3')
    >>> csvds.get((('R1C1', 'R1C3')), 'default')
    'default'

    The ``has_header`` flag can be used to avoid skipping the header:

    >>> source.seek(0)
    >>> from importtools.loaders import CSVLoader
    >>> csvl = CSVLoader(
    ...     lambda x, y: tuple([x, y]),
    ...     columns=[1,3],
    ...     has_header=False)
    >>> csvl = csvl.bind(source)
    >>> csvds = MemoryDataSet(csvl)
    >>> csvds.get((('R1C1', 'R1C3')))
    ('R1C1', 'R1C3')
    >>> csvds.get((('R2C1', 'R2C3')))
    ('R2C1', 'R2C3')
    >>> csvds.get((('R3C1', 'R3C3')))
    ('R3C1', 'R3C3')

    By default, ``has_header`` flag is set and the number of column can vary:

    >>> source.seek(0)
    >>> csvl = CSVLoader(
    ...     lambda x, y, z: tuple([x, y, z]),
    ...     columns=[1,2,3])
    >>> csvl = csvl.bind(source)
    >>> csvds = MemoryDataSet(csvl)
    >>> csvds.get((('R2C1', 'R2C2', 'R2C3')))
    ('R2C1', 'R2C2', 'R2C3')
    >>> csvds.get((('R3C1', 'R3C2', 'R3C3')))
    ('R3C1', 'R3C2', 'R3C3')
    >>> csvds.get((('R1C1', 'R1C2', 'R1C3')), 'default')
    'default'

    """

    def __init__(self, factory, columns, has_header=True):
        self.factory = factory
        self.columns = columns
        self.has_header = has_header
        self.source = []

    def bind(self, source):
        """Bind a data source to this loader."""
        self.source = source
        return self

    def _iter(self):
        """Yield instances returned by the factory.

        The factory is used to generate instances using the values of the
        selected column for each line in the ``CSV`` file.

        """
        source = self.source
        content = csv.reader(source)
        if self.has_header:
            content.next()
        for line in content:
            params = [line[column] for column in self.columns]
            yield self.factory(*params)

    def __iter__(self):
        return self._iter()


class ChunkedLoader(object):
    """A loading strategy for running large imports as multiple smaller ones.

    The main functionality of this loader is to create and populate in parallel
    two different ``DataSet`` instances with at most ``chunk_hint`` elements
    from two ordered generators always with the smaller values:

    >>> cl = ChunkedLoader(set, set, 5)
    >>> loader = cl.loader([10, 20, 30, 40], [11, 12, 50, 60])
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

    >>> loader = cl.loader([10, 20, 30, 40], [11, 12, 30, 60])
    >>> source, destination = loader.next()
    >>> sorted(source)
    [10, 20, 30]
    >>> sorted(destination)
    [11, 12, 30]

    In case the number of elements its divisible by the chunk size, the results
    should still be correct:

    >>> loader = cl.loader([10, 20, 30, 40, 50], [11, 12, 13, 60, 70])
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

    >>> loader = cl.loader([1, 2, 3, 4, 5, 6], [])
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
    >>> loader = cl.loader([], [1, 2, 3, 4, 5, 6])
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
    def __init__(self, source_factory, destination_factory, chunk_hint=16384):
        self.source_factory = source_factory
        self.destination_factory = destination_factory
        self.chunk_hint = chunk_hint

    def loader(self, source_loader, destination_loader):
        """
        A generator that can be used to load source and destination instances
        of :py:class:`~.importtools.datasets.DataSet`

        This generator yield source and destination tulpes.

        """
        w_source = self.dataset_wrapper(source_loader, True)
        w_destination = self.dataset_wrapper(destination_loader, False)
        merged = heapq.merge(w_source, w_destination)

        while True:
            source = self.source_factory()
            destination = self.destination_factory()
            current_chunk = itertools.islice(merged, self.chunk_hint)
            empty = True
            for importable, from_source in current_chunk:
                empty = False
                if from_source:
                    source.add(importable)
                else:
                    destination.add(importable)
            if empty:
                raise StopIteration

            try:
                next_importable, next_from_source = merged.next()
            except StopIteration:
                pass
            else:
                if next_importable == importable:
                    if next_from_source:
                        source.add(next_importable)
                    else:
                        destination.add(next_importable)
                else:
                    v = (next_importable, next_from_source)
                    merged = itertools.chain([v], merged)

            yield source, destination

    def dataset_wrapper(self, g, const):
        for value in g:
            yield value, const
