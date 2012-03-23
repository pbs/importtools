import abc


__all__ = ['RODataSet', 'DataSet', 'MemoryDataSet', 'DiffDataSet']


class RODataSet(object):
    """
    An :py:mod:`abc` that represents a read-only set of
    :py:class:`~.importtools.importables.Importable` instances.

    """

    @abc.abstractmethod
    def __iter__(self):
        """
        Iterate over all the :py:class:`~.importtools.importables.Importable`
        instances in this set.

        """

    @abc.abstractmethod
    def __contains__(self, importable):
        """
        Check if the dataset contains an equal
        :py:class:`~.importtools.importables.Importable` instance.

        See :py:meth:`~.importtools.importables.Importable.__cmp__` method for
        more details about the equality test between two
        :py:class:`~.importtools.importables.Importable` instances.

        """

    @abc.abstractmethod
    def get(self, importable, default=None):
        """
        Return an equal :py:class:`~.importtools.importables.Importable` from
        the dataset or the default value if no such instances are found.

        """


class DataSet(RODataSet):
    """
    An :py:mod:`abc` that represents a mutable set of
    :py:class:`~.importtools.importables.Importable` instances.

    """

    @abc.abstractmethod
    def add(self, importable):
        """
        Add the :py:class:`~.importtools.importables.Importable` in the dataset
        or replaces an existing and equal one.

        """

    @abc.abstractmethod
    def pop(self, importable):
        """
        Remove and return an equal
        :py:class:`~.importtools.importables.Importable` from the dataset.

        """


class MemoryDataSet(dict, DataSet):
    """
    A simple in-memory :py:class:`dict`-based :py:class:`DataSet`
    implementation.

    Since this particular :py:class:`DataSet` implementation only uses the
    :py:meth:`__hash__` and :py:meth:`__cmp__` methods of an
    :py:class:`~.importtools.importables.Importable` :py:mod:`abc` we can
    exemplify this class functionality using :py:class:`tuple` objects:

    >>> from importtools import MemoryDataSet
    >>> item1 = (1, )
    >>> item2 = (1, )
    >>> item3 = (2, )

    At first, a newly created :py:class:`MemoryDataSet` instance has no
    elements:

    >>> mds = MemoryDataSet()
    >>> list(mds)
    []

    After creation, it can be populated and the elements in the dataset can be
    retrieved using other equal elements.

    >>> mds = MemoryDataSet()
    >>> mds.add(item1)
    >>> mds.get(item1) == item1
    True
    >>> mds.get(item1) is item1
    True
    >>> mds.get(item2) == item2
    True
    >>> mds.get(item2) is item1
    True

    Trying to get an inexistent item should return the default value or
    :py:class:`None`:

    >>> mds = MemoryDataSet()
    >>> mds.add(item1)
    >>> mds.get(item3) is None
    True
    >>> mds.get(item3, 'default')
    'default'

    Initial data can be passed when constructing instances:

    >>> mds = MemoryDataSet((item1, item2, item3))

    """
    def __init__(self, data_loader=None, *args, **kwargs):
        data_iter = tuple()
        if data_loader is not None:
            data_iter = iter(data_loader)
        mapping = ((i, i) for i in data_iter)
        super(MemoryDataSet, self).__init__(mapping, *args, **kwargs)

    def add(self, importable):
        self[importable] = importable


class DiffDataSet(MemoryDataSet):
    """
    A :py:class:`DataSet` implementation that remembers all the changes,
    additions and removals done to it.

    Using a :py:class:`DiffDataSet` as the destination of the import algorithm
    allows optimal persistence of the changes by grouping them in a way suited
    for batch processing.

    In order to exemplify this class functionality we start with a
    instance that we prepopulate:

    An :py:class:`~.importtools.importables.Importable` mock is needed to
    populate the datasets:

    >>> from importtools.importables import MockImportable
    >>> from importtools import Importable, MemoryDataSet, DiffDataSet
    >>> dds = DiffDataSet([
    ...     MockImportable(0),
    ...     MockImportable(1),
    ...     MockImportable(2),
    ...     MockImportable(3)
    ... ])

    >>> sorted(dds)
    [<(0,) IMPORTED>, <(1,) IMPORTED>, <(2,) IMPORTED>, <(3,) IMPORTED>]

    Fetching existing elements from ``dds`` should work correctly:

    >>> dds.get(MockImportable(1)) == MockImportable(1)
    True

    New :py:class:`~.importtools.importables.Importable` can still added in
    ``dds``:

    >>> dds.add(MockImportable(4))
    >>> dds.add(MockImportable(5))
    >>> sorted(dds)
    ... # doctest: +NORMALIZE_WHITESPACE
    [<(0,) IMPORTED>, <(1,) IMPORTED>, <(2,) IMPORTED>,
     <(3,) IMPORTED>, <(4,) IMPORTED>, <(5,) IMPORTED>]

    Check if the new elements were added:

    >>> dds.get(MockImportable(4))
    <(4,) IMPORTED>
    >>> dds.get(MockImportable(5))
    <(5,) IMPORTED>

    A lookup with an inexistent element should return the default value:

    >>> dds.get(MockImportable(100), 'default')
    'default'

    We should be able to ``pop`` both original and recently added elements:

    >>> dds.pop(MockImportable(1))
    <(1,) IMPORTED>
    >>> dds.pop(MockImportable(4))
    <(4,) IMPORTED>
    >>> sorted(dds)
    [<(0,) IMPORTED>, <(2,) IMPORTED>, <(3,) IMPORTED>, <(5,) IMPORTED>]

    And everything should have been registered:

    >>> sorted(dds.get_added())
    [<(5,) IMPORTED>]
    >>> sorted(dds.get_removed())
    [<(1,) IMPORTED>]
    >>> sorted(dds.get_changed())
    []

    Adding and removing the same
    :py:class:`~.importtools.importables.Importable` shouldn't change anything:

    >>> dds.add(MockImportable(100))
    >>> dds.pop(MockImportable(100))
    <(100,) IMPORTED>
    >>> sorted(dds.get_added())
    [<(5,) IMPORTED>]
    >>> sorted(dds.get_removed())
    [<(1,) IMPORTED>]
    >>> sorted(dds.get_changed())
    []

    But removing an original :py:class:`~.importtools.importables.Importable`
    and adding a new one should be interpreted as a change:

    >>> dds.pop(MockImportable(2))
    <(2,) IMPORTED>
    >>> dds.add(MockImportable(2))
    >>> sorted(dds.get_added())
    [<(5,) IMPORTED>]
    >>> sorted(dds.get_removed())
    [<(1,) IMPORTED>]
    >>> sorted(dds.get_changed())
    [<(2,) IMPORTED>]

    If you change one of the existing elements it will be marked as changed:

    >>> dds.get(MockImportable(3)).register_change()
    >>> sorted(dds.get_added())
    [<(5,) IMPORTED>]
    >>> sorted(dds.get_removed())
    [<(1,) IMPORTED>]
    >>> sorted(dds.get_changed())
    [<(2,) IMPORTED>, <(3,) IMPORTED>]

    Even if the elements are accessed by iterating on over the dataset the
    changes are still tracked:

    >>> list(dds)[0].register_change()
    >>> sorted(dds.get_added())
    [<(5,) IMPORTED>]
    >>> sorted(dds.get_removed())
    [<(1,) IMPORTED>]
    >>> sorted(dds.get_changed())
    [<(0,) IMPORTED>, <(2,) IMPORTED>, <(3,) IMPORTED>]

    When printing the object a list of all the added, removed and created
    elements will be shown, along with the total number for each operation:

    >>> print dds
    To be added: 1
    <(5,) IMPORTED>
    To be removed: 1
    <(1,) IMPORTED>
    To be changed: 3
    <(0,) IMPORTED>
    <(2,) IMPORTED>
    <(3,) IMPORTED>

    >>> dds._added = MemoryDataSet()
    >>> dds._removed = MemoryDataSet()
    >>> print dds
    To be changed: 3
    <(0,) IMPORTED>
    <(2,) IMPORTED>
    <(3,) IMPORTED>

    >>> dds._changed = MemoryDataSet()
    >>> print dds
    No elements were affected.

    """
    def __init__(self, data_loader=None, *args, **kwargs):
        self._added = set()
        self._removed = set()
        self._changed = set()
        # XXX for some reason dict subclass methods can't be hashed
        self.rc = lambda x: self.register_change(x)
        super(DiffDataSet, self).__init__(
            element.register_listener(self.rc) for element in data_loader,
            *args, **kwargs
        )

    def __str__(self):
        result = ''
        added_count = len(self._added)
        if added_count > 0:
            result += 'To be added: %s\n' % added_count
            result += '\n'.join(sorted(repr(x) for x in self.get_added()))
        removed_count = len(self._removed)
        if removed_count > 0:
            if result:
                result += '\n'
            result += 'To be removed: %s\n' % removed_count
            result += '\n'.join(sorted(repr(x) for x in self.get_removed()))
        changed_count = len(self._changed)
        if changed_count > 0:
            if result:
                result += '\n'
            result += 'To be changed: %s\n' % changed_count
            result += '\n'.join(sorted(repr(x) for x in self.get_changed()))
        if (added_count + removed_count + changed_count) == 0:
            result += 'No elements were affected.'
        return result

    def add(self, importable):
        if importable in self._removed:
            self._removed.discard(importable)
            self._changed.add(importable)
        else:
            self._added.add(importable)
        super(DiffDataSet, self).add(importable.register_listener(self.rc))

    def pop(self, importable):
        if importable in self._added:
            self._added.discard(importable)
        else:
            self._removed.add(importable)
        return super(DiffDataSet, self).pop(importable)

    def register_change(self, importable):
        """Mark an element in the current dataset as changed.

        This method is registered as a listener for changes in all the
        elements of the wrapped :py:class:`DataSet`.

        """
        assert importable in self
        self._changed.add(importable)

    def get_added(self):
        """A set of all added elements in the wrapped dataset."""
        return self._added

    def get_removed(self):
        """A set of all removed elements in the wrapped dataset."""
        return self._removed

    def get_changed(self):
        """A set of all changed elements in the wrapped dataset."""
        return self._changed
