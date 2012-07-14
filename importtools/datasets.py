import abc
import itertools


__all__ = ['DataSet', 'SimpleDataSet']


class DataSet(object):
    """
    An :py:mod:`abc` that represents a mutable set that can hold
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
    def get(self, importable, default=None):
        """
        Return an equal :py:class:`~.importtools.importables.Importable` from
        the dataset or the default value if no such instances are found.

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


class SimpleDataSet(dict, DataSet):
    """A simple :py:class:`dict`-based :py:class:`DataSet` implementation.

    >>> from importtools import Importable
    >>> i1, i2, i3 = Importable(0), Importable(0), Importable(1)

    At first, a newly created :py:class:`MemoryDataSet` instance has no
    elements:

    >>> sds = SimpleDataSet()
    >>> list(sds)
    []

    After creation, it can be populated and the elements in the dataset can be
    retrieved using other equal elements.  Trying to ``get`` an inexistent item
    should return the default value or :py:class:`None`:

    >>> sds.add(i1)
    >>> sds.get(i1) is i1
    True
    >>> sds.get(i2) is i1
    True
    >>> sds.get(i3) is None
    True
    >>> sds.get(i3, 'default')
    'default'

    An iterable containing the initial data can be passed when constructing
    ``SimpleDataSet`` intances:

    >>> sds = SimpleDataSet((i1, i3))
    >>> sorted(list(sds))
    [Importable(0, ...), Importable(1, ...)]

    Initial data can not contain duplicates:

    >>> init_values = (i1, i2, i3)
    >>> SimpleDataSet(init_values) # doctest:+IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ValueError:

    """
    def __init__(self, data_loader=None, *args, **kwargs):
        data_iter = tuple()
        if data_loader is not None:
            data_iter = iter(data_loader)
        mapping = ((i, i) for i in data_iter)
        super(SimpleDataSet, self).__init__(mapping, *args, **kwargs)
        # When you create a dict and provide initial data if there are equal
        # elements in the initial list, the dict will have the key equal
        # to the first element and the value of the last duplicated element.
        err = 'The initial list for dataset can not contain duplicates: %s %s'
        # Try to detect if there are duplicates and fail if so because
        # we don't know which value sholud be used.
        for k, v in self.iteritems():
            if k is not v:
                raise ValueError(err % (k, v))

    def add(self, importable):
        self[importable] = importable


# class DiffDataSet(MemoryDataSet):
#     """
#     A :py:class:`DataSet` implementation that remembers all the changes,
#     additions and removals done to it.

#     Using a :py:class:`DiffDataSet` as the destination of the import algorithm
#     allows optimal persistence of the changes by grouping them in a way suited
#     for batch processing.

#     In order to exemplify this class functionality we start with a
#     instance that we prepopulate:

#     An :py:class:`~.importtools.importables.Importable` mock is needed to
#     populate the datasets:

#     >>> from importtools.importables import MockImportable
#     >>> from importtools import Importable, MemoryDataSet, DiffDataSet
#     >>> dds = DiffDataSet([
#     ...     MockImportable(id=0, a=0, b=0),
#     ...     MockImportable(id=1, a=1, b=1),
#     ...     MockImportable(id=2, a=2, b=2),
#     ...     MockImportable(id=3, a=3, b=3)
#     ... ])

#     >>> sorted(dds)
#     [<MI a 0, b 0>, <MI a 1, b 1>, <MI a 2, b 2>, <MI a 3, b 3>]

#     Fetching existing elements from ``dds`` should work correctly:

#     >>> dds.get(MockImportable(id=1, a=0, b=0))
#     <MI a 1, b 1>

#     New :py:class:`~.importtools.importables.Importable` can still added in
#     ``dds``:

#     >>> dds.add(MockImportable(id=4, a=4, b=4))
#     >>> dds.add(MockImportable(id=5, a=5, b=5))
#     >>> sorted(dds)
#     ... # doctest: +NORMALIZE_WHITESPACE
#     [<MI a 0, b 0>, <MI a 1, b 1>, <MI a 2, b 2>,
#      <MI a 3, b 3>, <MI a 4, b 4>, <MI a 5, b 5>]

#     Check if the new elements were added:

#     >>> dds.get(MockImportable(id=4, a=0, b=0))
#     <MI a 4, b 4>
#     >>> dds.get(MockImportable(id=5, a=0, b=0))
#     <MI a 5, b 5>

#     A lookup with an inexistent element should return the default value:

#     >>> dds.get(MockImportable(id=100, a=0, b=0), 'default')
#     'default'

#     We should be able to ``pop`` both original and recently added elements:

#     >>> dds.pop(MockImportable(id=1, a=0, b=0))
#     <MI a 1, b 1>
#     >>> dds.pop(MockImportable(id=4, a=0, b=0))
#     <MI a 4, b 4>
#     >>> sorted(dds)
#     [<MI a 0, b 0>, <MI a 2, b 2>, <MI a 3, b 3>, <MI a 5, b 5>]

#     And everything should have been registered:

#     >>> sorted(dds.added)
#     [<MI a 5, b 5>]
#     >>> sorted(dds.removed)
#     [<MI a 1, b 1>]
#     >>> sorted(dds.changed)
#     []

#     Adding and removing the same
#     :py:class:`~.importtools.importables.Importable` shouldn't change anything:

#     >>> dds.add(MockImportable(id=100, a=100, b=100))
#     >>> dds.pop(MockImportable(id=100, a=0, b=0))
#     <MI a 100, b 100>
#     >>> sorted(dds.added)
#     [<MI a 5, b 5>]
#     >>> sorted(dds.removed)
#     [<MI a 1, b 1>]
#     >>> sorted(dds.changed)
#     []

#     But removing an original :py:class:`~.importtools.importables.Importable`
#     and adding a new one should be interpreted as a change:

#     >>> dds.pop(MockImportable(id=2, a=0, b=0))
#     <MI a 2, b 2>
#     >>> dds.add(MockImportable(id=2, a=2, b=2))
#     >>> sorted(dds.added)
#     [<MI a 5, b 5>]
#     >>> sorted(dds.removed)
#     [<MI a 1, b 1>]
#     >>> sorted(dds.changed)
#     [<MI a 2, b 2>]

#     If you change one of the existing elements it will be marked as changed:

#     >>> dds.get(MockImportable(id=3, a=0, b=0)).register_change()
#     >>> sorted(dds.added)
#     [<MI a 5, b 5>]
#     >>> sorted(dds.removed)
#     [<MI a 1, b 1>]
#     >>> sorted(dds.changed)
#     [<MI a 2, b 2>, <MI a 3, b 3>]

#     Even if the elements are accessed by iterating on over the dataset the
#     changes are still tracked:

#     >>> list(dds)[0].register_change()
#     >>> sorted(dds.added)
#     [<MI a 5, b 5>]
#     >>> sorted(dds.removed)
#     [<MI a 1, b 1>]
#     >>> sorted(dds.changed)
#     [<MI a 0, b 0>, <MI a 2, b 2>, <MI a 3, b 3>]

#     When printing the object a list of all the added, removed and created
#     elements will be shown, along with the total number for each operation:

#     >>> print dds
#     <DiffDataSet: 3 changed, 1 added, 1 removed>

#     >>> dds._added = MemoryDataSet()
#     >>> dds._removed = MemoryDataSet()
#     >>> print dds
#     <DiffDataSet: 3 changed, 0 added, 0 removed>

#     >>> dds._changed = MemoryDataSet()
#     >>> print dds
#     <DiffDataSet: 0 changed, 0 added, 0 removed>

#     """
#     def __init__(self, data_loader=None, *args, **kwargs):
#         self._added = set()
#         self._removed = set()
#         self._changed = set()
        # XXX for some reason dict subclass methods can't be hashed
#         self.rc = lambda x: self.register_change(x)
#         super(DiffDataSet, self).__init__(
#             element.register_listener(self.rc) for element in data_loader,
#             *args, **kwargs
#         )

#     def __repr__(self):
#         result = '<%s: %d changed, %d added, %d removed>'
#         instance_cls = self.__class__.__name__
#         c = map(len, [self._changed, self._added, self._removed])
#         return result % tuple([instance_cls] + c)

#     def add(self, importable):
#         if importable in self._removed:
#             self._removed.discard(importable)
#             self._changed.add(importable)
#         else:
#             self._added.add(importable)
#         super(DiffDataSet, self).add(importable.register_listener(self.rc))

#     def pop(self, importable):
#         i = super(DiffDataSet, self).pop(importable)
#         if importable in self._added:
#             self._added.discard(importable)
#         else:
#             self._removed.add(i)
#         return i

#     def register_change(self, importable):
#         """Mark an element in the current dataset as changed.

#         This method is registered as a listener for changes in all the
#         elements of the wrapped :py:class:`DataSet`.

#         """
#         assert importable in self
#         self._changed.add(importable)

#     @property
#     def added(self):
#         """An iterable  of all added elements in the dataset."""
#         return iter(self._added)

#     @property
#     def removed(self):
#         """An iterable of all removed elements in the dataset."""
#         return iter(self._removed)

#     @property
#     def changed(self):
#         """An iterable of all changed elements in the dataset."""
#         return iter(self._changed)
