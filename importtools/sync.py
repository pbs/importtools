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
    def __init__(self, source_factory, destination_factory, chunk_hint=16384):
        self.source_factory = source_factory
        self.destination_factory = destination_factory
        self.chunk_hint = chunk_hint

    def loader(self, source_loader, destination_loader):
        w_source = self.dataset_wrapper(source_loader, True)
        w_destination = self.dataset_wrapper(destination_loader, False)
        merged = heapq.merge(w_source, w_destination)

        while True:
            source = self.source_factory()
            destination = self.destination_factory()
            current_chunk = itertools.islice(merged, 0, self.chunk_hint)
            for importable, from_source in current_chunk:
                if from_source:
                    source.add(importable)
                else:
                    destination.add(importable)
            else:
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
                    v = [(next_importable, next_from_source)]
                    merged = itertools.chain([v], merged)

            yield source, destination

    def dataset_wrapper(self, g, const):
        for value in g:
            yield value, const
