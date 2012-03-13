import abc
import csv


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

        See :py:meth:`~.importtools.importables.Importable.__eq__` method for
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
    :py:meth:`__hash__` and :py:meth:`__eq__` methods of an
    :py:class:`~.importtools.importables.Importable` :py:mod:`abc` we can
    exemplify this class functionality using :py:class:`tuple` objects:

    .. testsetup::

    >>> from importtools.datasets import MemoryDataSet
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

    """
    def add(self, importable):
        self[importable] = importable


class DiffDataSet(DataSet):
    """
    A :py:class:`DataSet` wrapper that remembers all the changes, additions and
    removals done to the wrapped dataset trough itself.

    Using a :py:class:`DiffDataSet` as the destination of the import algorithm
    allows optimal persistence of the changes by grouping them in a way suited
    for batch processing.

    In order to exemplify this class functionality we start with a
    :py:class:`MemoryDataSet` instance that we wrap:

    .. testsetup::

    >>> from importtools.datasets import MemoryDataSet, DiffDataSet
    >>> mds = MemoryDataSet()
    >>> dds = DiffDataSet(mds)

    An :py:class:`~.importtools.importables.Importable` mock is needed to
    populate the datasets:

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

    It's now possible to populate the wrapped :py:class:`MemoryDataSet`
    instance with some elements:

    >>> mds.add(MockImportable(0))
    >>> mds.add(MockImportable(1))
    >>> mds.add(MockImportable(2))
    >>> mds.add(MockImportable(3))
    >>> sorted(mds, key=lambda x: x.args)
    [<(0,) IMPORTED>, <(1,) IMPORTED>, <(2,) IMPORTED>, <(3,) IMPORTED>]

    The contents of ``dds`` should be the same as the content of the wrapped
    ``mds``:

    >>> sorted(dds, key=lambda x: x.args)
    [<(0,) IMPORTED>, <(1,) IMPORTED>, <(2,) IMPORTED>, <(3,) IMPORTED>]

    Fetching existing elements from ``dds`` should work the same as fetching
    elements from ``mds``:

    >>> mds.get(MockImportable(1)) == dds.get(MockImportable(1))
    True

    New :py:class:`~.importtools.importables.Importable` can still added in
    ``dds``:

    >>> dds.add(MockImportable(4))
    >>> dds.add(MockImportable(5))
    >>> sorted(dds, key=lambda x: x.args)
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
    >>> sorted(dds, key=lambda x: x.args)
    [<(0,) IMPORTED>, <(2,) IMPORTED>, <(3,) IMPORTED>, <(5,) IMPORTED>]

    And everything should have been registered:

    >>> sorted(dds.get_added(), key=lambda x: x.args)
    [<(5,) IMPORTED>]
    >>> sorted(dds.get_removed(), key=lambda x: x.args)
    [<(1,) IMPORTED>]
    >>> sorted(dds.get_changed(), key=lambda x: x.args)
    []

    Adding and removing the same
    :py:class:`~.importtools.importables.Importable` shouldn't change anything:

    >>> dds.add(MockImportable(100))
    >>> dds.pop(MockImportable(100))
    <(100,) IMPORTED>
    >>> sorted(dds.get_added(), key=lambda x: x.args)
    [<(5,) IMPORTED>]
    >>> sorted(dds.get_removed(), key=lambda x: x.args)
    [<(1,) IMPORTED>]
    >>> sorted(dds.get_changed(), key=lambda x: x.args)
    []

    But removing an original :py:class:`~.importtools.importables.Importable`
    and adding a new one should be interpreted as a change:

    >>> dds.pop(MockImportable(2))
    <(2,) IMPORTED>
    >>> dds.add(MockImportable(2))
    >>> sorted(dds.get_added(), key=lambda x: x.args)
    [<(5,) IMPORTED>]
    >>> sorted(dds.get_removed(), key=lambda x: x.args)
    [<(1,) IMPORTED>]
    >>> sorted(dds.get_changed(), key=lambda x: x.args)
    [<(2,) IMPORTED>]

    If you change one of the existing elements it will be marked as changed:

    >>> dds.get(MockImportable(3)).register_change()
    >>> sorted(dds.get_added(), key=lambda x: x.args)
    [<(5,) IMPORTED>]
    >>> sorted(dds.get_removed(), key=lambda x: x.args)
    [<(1,) IMPORTED>]
    >>> sorted(dds.get_changed(), key=lambda x: x.args)
    [<(2,) IMPORTED>, <(3,) IMPORTED>]

    Even if the elements are accessed by iterating on over the dataset the
    changes are still tracked:

    >>> list(dds)[0].register_change()
    >>> sorted(dds.get_added(), key=lambda x: x.args)
    [<(5,) IMPORTED>]
    >>> sorted(dds.get_removed(), key=lambda x: x.args)
    [<(1,) IMPORTED>]
    >>> sorted(dds.get_changed(), key=lambda x: x.args)
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
    <BLANKLINE>
    To be changed: 3
    <(0,) IMPORTED>
    <(2,) IMPORTED>
    <(3,) IMPORTED>

    >>> dds._changed = MemoryDataSet()
    >>> print dds
    No elements were affected.

    """
    def __init__(self, data_set):
        self._wrapped = data_set
        self._added = MemoryDataSet()
        self._removed = MemoryDataSet()
        self._changed = MemoryDataSet()
        super(DiffDataSet, self).__init__()

    def __iter__(self):
        for iterable in self._wrapped:
            yield iterable.register_listener(self.register_change)

    def __contains__(self, importable):
        return importable in self._wrapped

    def __str__(self):
        result = ''

        added_count = len(self._added)
        if added_count > 0:
            result += 'To be added: %s\n' % added_count
            result += '\n'.join(sorted(repr(x) for x in self.get_added()))

        removed_count = len(self._removed)
        if removed_count > 0:
            result += '\nTo be removed: %s\n' % removed_count
            result += '\n'.join(sorted(repr(x) for x in self.get_removed()))

        changed_count = len(self._changed)
        if changed_count > 0:
            result += '\nTo be changed: %s\n' % changed_count
            result += '\n'.join(sorted(repr(x) for x in self.get_changed()))

        if (added_count + removed_count + changed_count) == 0:
            result += 'No elements were affected.'

        return result

    def get(self, importable, default=None):
        importable = self._wrapped.get(importable)
        if importable is None:
            return default
        return importable.register_listener(self.register_change)

    def add(self, importable):
        self._wrapped.add(importable)

        was_removed = self._removed.get(importable)
        if was_removed is None:
            self._added.add(importable)
        else:
            self._removed.pop(importable)
            self._changed.add(importable)

    def pop(self, importable):
        was_added = self._added.get(importable)
        if was_added is None:
            self._removed.add(importable)
        else:
            self._added.pop(importable)

        return self._wrapped.pop(importable)

    def register_change(self, importable):
        """Mark an element in the current dataset as changed.

        This method is registered as a listener for changes in all the
        elements of the wrapped :py:class:`DataSet`.

        """
        assert importable in self._wrapped
        self._changed.add(importable)

    def get_added(self):
        """An iterable of all added elements in the wrapped dataset."""
        return iter(self._added)

    def get_removed(self):
        """An iterable of all removed elements in the wrapped dataset."""
        return iter(self._removed)

    def get_changed(self):
        """An iterable of all changed elements in the wrapped dataset."""
        return iter(self._changed)


class ArgsDataSet(MemoryDataSet):
    """
    A :py:class:`DataSet` implementation that gets prepopulated from a
    flattened list by grouping values together and instantiating a factory.
    The primary use of this implementation is to create a datasets from command
    line arguments.

    This is a short example where each two consecutive values are grouped
    together and used to create tuple instances from a factory function.

    >>> from importtools.datasets import ArgsDataSet
    >>> argsds = ArgsDataSet(lambda x, y: tuple([x, y]), 2)
    >>> argsds.populate(['STATE1', 'CL1', 'STATE2', 'CL2'])
    >>> argsds.get(('STATE1', 'CL1'))
    ('STATE1', 'CL1')
    >>> sorted(argsds)
    [('STATE1', 'CL1'), ('STATE2', 'CL2')]

    """
    def __init__(self, factory, how_many=1):
        super(ArgsDataSet, self).__init__()
        self.factory = factory
        self.how_many = how_many

    def populate(self, args):
        """Populate the dataset with instances returned by the factory.

        The factory is called with ``args`` values grouped together as dictated
        by the value of ``how_many`` property.

        """
        assert len(args) % self.how_many == 0
        self.clear()
        # http://docs.python.org/library/itertools.html#recipes
        arg_groups = zip(*[iter(args)] * self.how_many)
        for arg_group in arg_groups:
            self.add(self.factory(*arg_group))


class CSVDataSet(MemoryDataSet):
    """
    A :py:class:`DataSet` implementation that gets prepopulated from a ``CSV``
    file and instantiate :py:class:`~.importtools.importables.Importable`
    instances.

    >>> import StringIO
    >>> source = StringIO.StringIO('''
    ... R1C0,R1C1,R1C2,R1C3
    ... R2C0,R2C1,R2C2,R2C3
    ... R3C0,R3C1,R3C2,R3C3
    ... '''.strip())

    The file is parsed skipping the header and the ``DataSet`` is populated
    with the correct values:

    >>> source.seek(0)
    >>> csvds = CSVDataSet(
    ...     lambda x, y: tuple([x, y]),
    ...     columns=[1,3],
    ...     has_header=True)
    >>> csvds.populate(source)
    >>> csvds.get((('R2C1', 'R2C3')))
    ('R2C1', 'R2C3')
    >>> csvds.get((('R3C1', 'R3C3')))
    ('R3C1', 'R3C3')
    >>> csvds.get((('R1C1', 'R1C3')), 'default')
    'default'

    The ``has_header`` flag can be used to avoid skipping the header:

    >>> source.seek(0)
    >>> from importtools.datasets import CSVDataSet
    >>> csvds = CSVDataSet(
    ...     lambda x, y: tuple([x, y]),
    ...     columns=[1,3],
    ...     has_header=False)
    >>> csvds.populate(source)
    >>> csvds.get((('R1C1', 'R1C3')))
    ('R1C1', 'R1C3')
    >>> csvds.get((('R2C1', 'R2C3')))
    ('R2C1', 'R2C3')
    >>> csvds.get((('R3C1', 'R3C3')))
    ('R3C1', 'R3C3')

    By default, ``has_header`` flag is set and the number of column can vary:

    >>> source.seek(0)
    >>> csvds = CSVDataSet(
    ...     lambda x, y, z: tuple([x, y, z]),
    ...     columns=[1,2,3])
    >>> csvds.populate(source)
    >>> csvds.get((('R2C1', 'R2C2', 'R2C3')))
    ('R2C1', 'R2C2', 'R2C3')
    >>> csvds.get((('R3C1', 'R3C2', 'R3C3')))
    ('R3C1', 'R3C2', 'R3C3')
    >>> csvds.get((('R1C1', 'R1C2', 'R1C3')), 'default')
    'default'

    """

    def __init__(self, factory, columns, has_header=True):
        super(CSVDataSet, self).__init__()
        self.factory = factory
        self.columns = columns
        self.has_header = has_header

    def populate(self, source):
        """Populate the dataset with instances returned by the factory.

        The factory is used to populate the :py:class:`DataSet` using the
        values of the selected column for each line in the ``CSV`` file.

        """
        self.clear()
        content = csv.reader(source)
        if self.has_header:
            content.next()
        for line in content:
            params = [line[column] for column in self.columns]
            self.add(self.factory(*params))
