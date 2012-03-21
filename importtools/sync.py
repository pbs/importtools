def full_sync(source, destination):
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

    >>> from importtools import MemoryDataSet
    >>> destination = MemoryDataSet()
    >>> source = MemoryDataSet()


    The destination and the source can now be populated with elements and the
    import should work like expected:

    >>> from importtools import Importable, MockImportable, full_sync

    >>> destination.add(MockImportable('i1'))
    >>> destination.add(MockImportable('i2'))
    >>> source.add(MockImportable('i2'))
    >>> source.add(MockImportable('i3'))
    >>> full_sync(source, destination)
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
    >>> full_sync(source, destination)
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
    >>> full_sync(source, destination)
    >>> sorted(destination, key=lambda x: x.args)
    [<('i',) FORCED>]

    """
    additive_sync(source, destination)

    to_delete = []
    for existing in destination:
        entry = source.get(existing)
        if entry is None and not existing.is_forced():
            to_delete.append(existing)

    for entry in to_delete:
        destination.pop(entry)


def additive_sync(source, destination):
    """
    A similar strategy with :py:class:`full_sync` but skips the deletion step
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

    >>> from importtools import MemoryDataSet
    >>> destination = MemoryDataSet()
    >>> source = MemoryDataSet()

    The destination and the source can now be populated with elements and the
    import should work like expected:

    >>> from importtools import Importable, MockImportable, additive_sync

    >>> destination.add(MockImportable('i1'))
    >>> destination.add(MockImportable('i2'))
    >>> source.add(MockImportable('i2'))
    >>> source.add(MockImportable('i3'))
    >>> additive_sync(source, destination)
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
    >>> additive_sync(source, destination)
    >>> sorted(destination, key=lambda x: x.args)
    [<('i',) IMPORTED>]

    If the source is empty nothing should change:

    >>> destination = MemoryDataSet()
    >>> source = MemoryDataSet()
    >>> destination.add(MockImportable('i1'))
    >>> destination.add(MockImportable('i2'))
    >>> destination.add(MockImportable('i3'))
    >>> additive_sync(source, destination)
    >>> sorted(destination, key=lambda x: x.args)
    [<('i1',) IMPORTED>, <('i2',) IMPORTED>, <('i3',) IMPORTED>]

    """
    for entry in source:
        existing = destination.get(entry)
        if existing is None:
            destination.add(entry)
        elif existing.is_forced():
            existing.make_imported()
