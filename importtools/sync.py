import heapq
import itertools


class FullSync(object):
    """
    An import strategy that adds the new elements from source in destination
    and deletes from the destination the elements that are not present in the
    source anymore.  The import algorithm uses a
    :py:class:`~.importtools.datasets.RODataSet` implementation as the data
    source and a :py:class:`~.importtools.datasets.DataSet` implementation as
    the destination.

    The algorithm is summarized in the following table:

    +---------+--------------+----------------------------------------------+
    |In source|In destination|Action                                        |
    +=========+==============+==============================================+
    |``True`` |``Imported``  |Nothing changes.                              |
    +---------+--------------+----------------------------------------------+
    |``True`` |``Invalid``   |Nothing changes, still invalid in destinaiton.|
    +---------+--------------+----------------------------------------------+
    |``True`` |``Forced``    |Change in destination to ``Imported``.        |
    +---------+--------------+----------------------------------------------+
    |``True`` |``False``     |Add in destination as ``Imported``.           |
    +---------+--------------+----------------------------------------------+
    |``False``|``Imported``  |Delete from destination.                      |
    +---------+--------------+----------------------------------------------+
    |``False``|``Invalid``   |Delete from destination.                      |
    +---------+--------------+----------------------------------------------+
    |``False``|``Forced``    |Nothing changes, keep ``Forced``.             |
    +---------+--------------+----------------------------------------------+

    In order to see the import in action we need to create a source and a
    destination, for the simplicity sake two
    :py:class:`~.importtools.datasets.MemoryDataSet` instances are used:

    .. testsetup::

    >>> from importtools.datasets import MemoryDataSet
    >>> destination = MemoryDataSet()
    >>> source = MemoryDataSet()

    An :py:class:`~.importtools.importables.Importable` mock is needed in order
    to exemplify the syncronization algorithms:

    .. testsetup::

    >>> from importtools.importables import Importable
    >>> class MockImportable(Importable):
    ...     def __init__(self, *args):
    ...         super(MockImportable, self).__init__()
    ...         self.args = tuple(args)
    ...         self.make_imported()
    ...     def __hash__(self):
    ...         return hash(self.args)
    ...     def __eq__(self, other):
    ...         return self.args == other.args
    ...     def __repr__(self):
    ...         smap = {1: 'IMPORTED', 2: 'FORCED', 3:'INVALID'}
    ...         return '<%r %s>' % (self.args, smap.get(self._status, 'N/A'))

    The destination and the source can now be populated with elements and the
    import should work like expected:

    .. testsetup::

    >>> from importtools.sync import FullSync

    >>> destination.add(MockImportable('i1'))
    >>> destination.add(MockImportable('i2'))
    >>> source.add(MockImportable('i2'))
    >>> source.add(MockImportable('i3'))
    >>> FullSync().run(source, destination)
    >>> sorted(destination, key=lambda x: x.args)
    [<('i2',) IMPORTED>, <('i3',) IMPORTED>]

    In case the destination contains a ``Forced``
    :py:class:`~.importtools.importables.Importable`` that is now available in
    source the element will be changed to ``Imported``:

    >>> destination = MemoryDataSet()
    >>> source = MemoryDataSet()
    >>> fmi = MockImportable('i')
    >>> fmi.make_forced()
    >>> destination.add(fmi)
    >>> source.add(MockImportable('i'))
    >>> FullSync().run(source, destination)
    >>> sorted(destination, key=lambda x: x.args)
    [<('i',) IMPORTED>]

    In case the destination contains a ``Forced``
    :py:class:`~.importtools.importables.Importable` that is not available in
    the source, the element will not be changed or deleted:

    >>> destination = MemoryDataSet()
    >>> source = MemoryDataSet()
    >>> fmi = MockImportable('i')
    >>> fmi.make_forced()
    >>> destination.add(fmi)
    >>> FullSync().run(source, destination)
    >>> sorted(destination, key=lambda x: x.args)
    [<('i',) FORCED>]

    """
    def run(self, source, destination):
        for entry in source:
            existing = destination.get(entry)
            if existing is None:
                destination.add(entry)
            elif existing.is_forced():
                existing.make_imported()

        to_delete = []
        for existing in destination:
            entry = source.get(existing)
            if entry is None and not existing.is_forced():
                to_delete.append(existing)

        for entry in to_delete:
            destination.pop(entry)


class AdditiveSync(object):
    """
    A similar strategy with :py:class:`FullSync` but skips the deletion step
    (i.e. if an element is in the destination but not in the source it's not
    removed).

    The algorithm is summarized in the following table:

    +---------+--------------+----------------------------------------------+
    |In source|In destination|Action                                        |
    +=========+==============+==============================================+
    |``True`` |``Imported``  |Nothing changes.                              |
    +---------+--------------+----------------------------------------------+
    |``True`` |``Invalid``   |Nothing changes, still invalid in destinaiton.|
    +---------+--------------+----------------------------------------------+
    |``True`` |``Forced``    |Change in destination to ``Imported``.        |
    +---------+--------------+----------------------------------------------+
    |``True`` |``False``     |Add in destination as ``Imported``.           |
    +---------+--------------+----------------------------------------------+
    |``False``|``Imported``  |Nothing changes.                              |
    +---------+--------------+----------------------------------------------+
    |``False``|``Invalid``   |Nothing changes, keep ``Invalid``.            |
    +---------+--------------+----------------------------------------------+
    |``False``|``Forced``    |Nothing changes, keep ``Forced``.             |
    +---------+--------------+----------------------------------------------+

    In order to see the import in action we need to create a source and a
    destination, for the simplicity sake two
    :py:class:`~.importtools.datasets.MemoryDataSet` instances are used:

    .. testsetup::

    >>> from importtools.datasets import MemoryDataSet
    >>> destination = MemoryDataSet()
    >>> source = MemoryDataSet()

    An :py:class:`~.importtools.importables.Importable` mock is needed in order
    to exemplify the syncronization algorithms:

    .. testsetup::

    >>> from importtools.importables import Importable
    >>> class MockImportable(Importable):
    ...     def __init__(self, *args):
    ...         super(MockImportable, self).__init__()
    ...         self.args = tuple(args)
    ...         self.make_imported()
    ...     def __hash__(self):
    ...         return hash(self.args)
    ...     def __eq__(self, other):
    ...         return self.args == other.args
    ...     def __repr__(self):
    ...         smap = {1: 'IMPORTED', 2: 'FORCED', 3:'INVALID'}
    ...         return '<%r %s>' % (self.args, smap.get(self._status, 'N/A'))

    The destination and the source can now be populated with elements and the
    import should work like expected:

    .. testsetup::

    >>> from importtools.sync import AdditiveSync

    >>> destination.add(MockImportable('i1'))
    >>> destination.add(MockImportable('i2'))
    >>> source.add(MockImportable('i2'))
    >>> source.add(MockImportable('i3'))
    >>> AdditiveSync().run(source, destination)
    >>> sorted(destination, key=lambda x: x.args)
    [<('i1',) IMPORTED>, <('i2',) IMPORTED>, <('i3',) IMPORTED>]

    In case the destination contains a ``Forced``
    :py:class:`~.importtools.importables.Importable`` that is now available in
    source the element will be changed to ``Imported``:

    >>> destination = MemoryDataSet()
    >>> source = MemoryDataSet()
    >>> fmi = MockImportable('i')
    >>> fmi.make_forced()
    >>> destination.add(fmi)
    >>> source.add(MockImportable('i'))
    >>> AdditiveSync().run(source, destination)
    >>> sorted(destination, key=lambda x: x.args)
    [<('i',) IMPORTED>]

    If the source is empty nothing should change:

    >>> destination = MemoryDataSet()
    >>> source = MemoryDataSet()
    >>> destination.add(MockImportable('i1'))
    >>> destination.add(MockImportable('i2'))
    >>> destination.add(MockImportable('i3'))
    >>> AdditiveSync().run(source, destination)
    >>> sorted(destination, key=lambda x: x.args)
    [<('i1',) IMPORTED>, <('i2',) IMPORTED>, <('i3',) IMPORTED>]

    """
    def run(self, source, destination):
        for entry in source:
            existing = destination.get(entry)
            if existing is None:
                destination.add(entry)
            elif existing.is_forced():
                existing.make_imported()


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
