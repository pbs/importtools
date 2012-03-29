__all__ = ['full_sync', 'add_sync', 'pop_sync', 'update_sync']


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
    add_sync(source, destination)
    pop_sync(source, destination)
    update_sync(source, destination)


def add_sync(source, destination):
    source_set = set(source)
    destination_set = set(destination)
    map(destination.add, source_set - destination_set)


def pop_sync(source, destination):
    source_set = set(source)
    destination_set = set(destination)
    map(destination.pop, destination_set - source_set)


def update_sync(source, destination):
    source_set = set(source)
    destination_set = set(destination)
    for element in source_set & destination_set:
        s_element = source.get(element)
        d_element = destination.get(element)
        d_element.update(s_element)
