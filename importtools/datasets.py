import abc
import itertools


__all__ = [
    'RODataSet', 'DataSet', 'MemoryDataSet', 'DiffDataSet', 'FilterDataSet'
]


class RODataSet(object):
    """
    An :py:mod:`abc` that represents a read-only set of
    :py:class:`~.importtools.importables.Importable` instances.

    """
    __metaclass__ = abc.ABCMeta

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
    ...     MockImportable(id=0, a=0, b=0),
    ...     MockImportable(id=1, a=1, b=1),
    ...     MockImportable(id=2, a=2, b=2),
    ...     MockImportable(id=3, a=3, b=3)
    ... ])

    >>> sorted(dds)
    [<MI a 0, b 0>, <MI a 1, b 1>, <MI a 2, b 2>, <MI a 3, b 3>]

    Fetching existing elements from ``dds`` should work correctly:

    >>> dds.get(MockImportable(id=1, a=0, b=0))
    <MI a 1, b 1>

    New :py:class:`~.importtools.importables.Importable` can still added in
    ``dds``:

    >>> dds.add(MockImportable(id=4, a=4, b=4))
    >>> dds.add(MockImportable(id=5, a=5, b=5))
    >>> sorted(dds)
    ... # doctest: +NORMALIZE_WHITESPACE
    [<MI a 0, b 0>, <MI a 1, b 1>, <MI a 2, b 2>,
     <MI a 3, b 3>, <MI a 4, b 4>, <MI a 5, b 5>]

    Check if the new elements were added:

    >>> dds.get(MockImportable(id=4, a=0, b=0))
    <MI a 4, b 4>
    >>> dds.get(MockImportable(id=5, a=0, b=0))
    <MI a 5, b 5>

    A lookup with an inexistent element should return the default value:

    >>> dds.get(MockImportable(id=100, a=0, b=0), 'default')
    'default'

    We should be able to ``pop`` both original and recently added elements:

    >>> dds.pop(MockImportable(id=1, a=0, b=0))
    <MI a 1, b 1>
    >>> dds.pop(MockImportable(id=4, a=0, b=0))
    <MI a 4, b 4>
    >>> sorted(dds)
    [<MI a 0, b 0>, <MI a 2, b 2>, <MI a 3, b 3>, <MI a 5, b 5>]

    And everything should have been registered:

    >>> sorted(dds.added)
    [<MI a 5, b 5>]
    >>> sorted(dds.removed)
    [<MI a 1, b 1>]
    >>> sorted(dds.changed)
    []

    Adding and removing the same
    :py:class:`~.importtools.importables.Importable` shouldn't change anything:

    >>> dds.add(MockImportable(id=100, a=100, b=100))
    >>> dds.pop(MockImportable(id=100, a=0, b=0))
    <MI a 100, b 100>
    >>> sorted(dds.added)
    [<MI a 5, b 5>]
    >>> sorted(dds.removed)
    [<MI a 1, b 1>]
    >>> sorted(dds.changed)
    []

    But removing an original :py:class:`~.importtools.importables.Importable`
    and adding a new one should be interpreted as a change:

    >>> dds.pop(MockImportable(id=2, a=0, b=0))
    <MI a 2, b 2>
    >>> dds.add(MockImportable(id=2, a=2, b=2))
    >>> sorted(dds.added)
    [<MI a 5, b 5>]
    >>> sorted(dds.removed)
    [<MI a 1, b 1>]
    >>> sorted(dds.changed)
    [<MI a 2, b 2>]

    If you change one of the existing elements it will be marked as changed:

    >>> dds.get(MockImportable(id=3, a=0, b=0)).register_change()
    >>> sorted(dds.added)
    [<MI a 5, b 5>]
    >>> sorted(dds.removed)
    [<MI a 1, b 1>]
    >>> sorted(dds.changed)
    [<MI a 2, b 2>, <MI a 3, b 3>]

    Even if the elements are accessed by iterating on over the dataset the
    changes are still tracked:

    >>> list(dds)[0].register_change()
    >>> sorted(dds.added)
    [<MI a 5, b 5>]
    >>> sorted(dds.removed)
    [<MI a 1, b 1>]
    >>> sorted(dds.changed)
    [<MI a 0, b 0>, <MI a 2, b 2>, <MI a 3, b 3>]

    When printing the object a list of all the added, removed and created
    elements will be shown, along with the total number for each operation:

    >>> print dds
    <DiffDataSet: 3 changed, 1 added, 1 removed>

    >>> dds._added = MemoryDataSet()
    >>> dds._removed = MemoryDataSet()
    >>> print dds
    <DiffDataSet: 3 changed, 0 added, 0 removed>

    >>> dds._changed = MemoryDataSet()
    >>> print dds
    <DiffDataSet: 0 changed, 0 added, 0 removed>

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
        result = '<%s: %d changed, %d added, %d removed>'
        instance_cls = self.__class__.__name__
        c = map(len, [self._changed, self._added, self._removed])
        return result % tuple([instance_cls] + c)

    def add(self, importable):
        if importable in self._removed:
            self._removed.discard(importable)
            self._changed.add(importable)
        else:
            self._added.add(importable)
        super(DiffDataSet, self).add(importable.register_listener(self.rc))

    def pop(self, importable):
        i = super(DiffDataSet, self).pop(importable)
        if importable in self._added:
            self._added.discard(importable)
        else:
            self._removed.add(i)
        return i

    def register_change(self, importable):
        """Mark an element in the current dataset as changed.

        This method is registered as a listener for changes in all the
        elements of the wrapped :py:class:`DataSet`.

        """
        assert importable in self
        self._changed.add(importable)

    @property
    def added(self):
        """An iterable  of all added elements in the dataset."""
        return iter(self._added)

    @property
    def removed(self):
        """An iterable of all removed elements in the dataset."""
        return iter(self._removed)

    @property
    def changed(self):
        """An iterable of all changed elements in the dataset."""
        return iter(self._changed)


class FilterDataSet(DataSet):
    """
    >>> from importtools import MemoryDataSet
    >>> m = MemoryDataSet(range(10))
    >>> f = FilterDataSet(m, lambda x: x % 2)
    >>> list(f)
    [1, 3, 5, 7, 9]

    """
    def __init__(self, dataset, is_valid):
        self.dataset = dataset
        self.is_valid = is_valid

    def __iter__(self):
        return itertools.ifilter(self.is_valid, self.dataset)

    def __contains__(self, importable):
        return (
            self.is_valid(importable)
            and importable in self.dataset
        )

    def get(self, importable, default=None):
        if self.is_valid(importable):
            return self.dataset.get(importable, default)
        return default

    def add(self, importable):
        self.dataset.add(importable)

    def pop(self, importable):
        self.dataset.pop(importable)
