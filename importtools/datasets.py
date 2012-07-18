"""This module contains ``DataSet`` implementations used to hold Importables.

The :py:class:`DataSet` concrete classes should be used when running import
algorithms as the destination. Before starting the import it should contain the
initial data found in the destination system and after running the import it
will contain the newly synchronized data. From this point of view this
structure serves as both input and output argument for the algorithm.

"""

import abc


__all__ = ['DataSet', 'SimpleDataSet']


class DataSet(object):
    """An ``abc`` that represents a mutable set of elements.

    This class serves as documentation of the methods a ``DataSet`` should
    implement. For concrete implementations available in this module see
    :py:class:`SimpleDataSet` and :py:class:`RecordingDataSet`.

    A `DataSet` is very similar with a normal `set` the difference being that
    you can :py:meth:`get` an element. This is useful is because if the
    elements are ``Importable`` instances even if they are equal (the natural
    keys are the same) the contents may be different.

    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __iter__(self):
        """Iterate over all the content of this set."""

    @abc.abstractmethod
    def get(self, element, default=None):
        """Return an equal element from the dataset or the default value."""

    @abc.abstractmethod
    def add(self, element):
        """Add or replace the element in the dataset."""

    @abc.abstractmethod
    def pop(self, element, default=None):
        """Remove and return an equal element from the dataset."""

    @abc.abstractmethod
    def clear(self):
        """Empty the dataset."""


class SimpleDataSet(dict, DataSet):
    """A simple :py:class:`dict`-based :py:class:`DataSet` implementation.

    At first, a newly created instance has no elements:

    >>> from importtools import Importable
    >>> i1, i2, i3 = Importable(0), Importable(0), Importable(1)
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
    >>> sds.pop(i3, 'default')
    'default'
    >>> sds.pop(i1)
    Importable(0)

    An iterable containing the initial data can be passed when constructing
    intances:

    >>> SimpleDataSet((i1, i3))
    SimpleDataSet([Importable(0), Importable(1)])

    A `ValueError` should be raised if the initial data contains duplicates:

    >>> init_values = (i1, i2, i3)
    >>> SimpleDataSet(init_values) # doctest:+IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ValueError:

    """
    def __init__(self, data_loader=None, *args, **kwargs):
        if data_loader is None:
            data_loader = ()
        mapping = ((i, i) for i in data_loader)
        super(SimpleDataSet, self).__init__(mapping, *args, **kwargs)
        # When a new dict is created and initial data is passed in the
        # constructor if there are elements in the initial data the dict will
        # have the key pointing to the first element and the value pointing to
        # the last duplicated element. We can use this trick to ensure that
        # there are not duplicates.
        # The reason we can't handle duplicate elements is because we don't
        # know which one of them to use.
        err = 'The initial list for dataset can not contain duplicates: %s %s'
        for k, v in self.iteritems():
            if k is not v:
                raise ValueError(err % (k, v))

    def add(self, element):
        self[element] = element

    def pop(self, element, default=None):
        # By default the dict pop doesn't take a default and raises key error.
        return super(SimpleDataSet, self).pop(element, default)

    def __iter__(self):
        return iter(self.values())

    def __repr__(self):
        """
        >>> SimpleDataSet()
        SimpleDataSet([])

        >>> SimpleDataSet([1, 2, 3])
        SimpleDataSet([1, 2, 3])

        >>> class TestDataSet(SimpleDataSet): pass
        >>> TestDataSet()
        TestDataSet([])

        """
        cls_name = self.__class__.__name__
        return '%s(%r)' % (cls_name, sorted(list(self)))


class RecordingDataSet(SimpleDataSet):
    """
    A :py:class:`DataSet` implementation that remembers all the changes,
    additions and removals done to it.

    Using a :py:class:`DiffDataSet` as the destination of the import algorithm
    allows optimal persistence of the changes by grouping them in a way suited
    for batch processing.

    """

    _sentinel = object()

    def __init__(self, data_loader=tuple(), *args, **kwargs):
        self._added = SimpleDataSet()
        self._removed = SimpleDataSet()
        self._changed = set()
        super(SimpleDataSet, self).__init__(
            self._registered_elements(data_loader),
            *args, **kwargs
        )

    def _registered_elements(self, data_loader):
        rc = self.register_change
        for element in data_loader:
            element.register_listener(rc)
            yield element

    def add(self, element):
        """

        >>> rds = RecordingDataSet()
        >>> rds.add(1)
        >>> rds.add(2)
        >>> sorted(list(rds.added))
        [1, 2]
        >>> rds.clear()
        >>> t1, t2, t3 = tuple([1]), tuple([1]), tuple([1])
        >>> rds.add(t1)
        >>> next(rds.added) is t1
        True
        >>> rds.add(t2)
        >>> next(rds.added) is t2
        True
        >>> rds.reset()
        >>> rds.add(t1)
        >>> next(rds.added) is t1
        True
        >>> next(rds.removed) is t2
        True
        >>> rds.add(t2)
        >>> len(list(rds.added)) is 0
        True
        >>> len(list(rds.removed)) is 0
        True
        >>> rds.add(t1)
        >>> rds.add(t3)
        >>> next(rds.added) is t3
        True
        >>> next(rds.removed) is t2
        True

        """

        sentinel = self._sentinel
        existing = self.get(element, sentinel)

        # Tring to add the same element that is already in the set. Nothing
        # should happen.
        if existing is element:
            return

        d_element = self._removed.get(element, sentinel)

        # We are adding back an element that was part of the set from the
        # beginning. Forget all possible changes recorded.
        if d_element is element:
            self._removed.pop(element)
            self._added.pop(element)
            super(RecordingDataSet, self).add(element)
            return

        a_element = self._added.get(element, sentinel)

        # If we are replacing an original element mark it as deleted.
        if a_element is sentinel and existing is not sentinel:
            self._removed.add(existing)

        self._added.add(element)
        super(RecordingDataSet, self).add(element)

    def pop(self, element, default=None):
        pass

    def register_change(self, element):
        """Mark an element in the current dataset as changed.

        This method is registered as a listener for changes in all the
        elements of the wrapped :py:class:`DataSet`.

        """
        assert element in self
        self._changed.add(element)

    def clear(self):
        self.reset()
        super(RecordingDataSet, self).clear()

    def reset(self):
        self._added.clear()
        self._removed.clear()
        self._changed.clear()

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
