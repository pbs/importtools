import abc


class Importable(object):
    """An :py:mod:`abc` that represents an element that can be imported.

    This :py:mod:`abc` has two abstract methods that need to be overridden by
    subclasses:

    * :py:meth:`__hash__`
    * :py:meth:`__cmp__`

    At first, a newly created :py:class:`Importable` has no state attached to
    it and later can be in one of the following states:

    .. _Imported:
    .. _Forced:
    .. _Invalid:

    +-------------+--------------------------------------------------------+
    |State        |Description                                             |
    +=============+========================================================+
    |``Imported`` | The entity comes from a typical import.                |
    +-------------+--------------------------------------------------------+
    |``Forced``   | The entity was manually added. It has high priority in |
    |             | the system since it's assumed to be correct.           |
    +-------------+--------------------------------------------------------+
    |``Invalid``  | The entity was imported but manually invalidated. It   |
    |             | has high priority in the system.                       |
    +-------------+--------------------------------------------------------+

    When the mutable part of an :py:class:`Importable` changes all the
    registered listeners, if any, must be notified. See
    :py:meth:`register_listener` and :py:meth:`register_change`.

    Each :py:class:`Importable` must know how to generate a hash based on its
    own natural keys. They must also define the equality comparison.  Two
    :py:class:`Importable` instances are considered equal only if their natural
    keys are the same.

    To exemplify the default functionality we first need to create a class that
    override all abstract methods:

    .. testsetup::

    >>> from importtools.importables import Importable
    >>> class MockImportable(Importable):
    ...     def __hash__(self):
    ...         return 0
    ...     def __cmp__(self, other):
    ...         return True
    ...     def __repr__(self):
    ...         return '<MockImportable %s>' % id(self)

    At first, a newly created :py:class:`Importable` has no state:

    >>> mi = MockImportable()
    >>> mi.is_imported(), mi.is_forced(), mi.is_invalid()
    (False, False, False)

    The entity can be marked as ``Imported``, ``Forced`` and ``Invalid``:

    >>> mi = MockImportable()
    >>> mi.make_imported()
    >>> mi.is_imported(), mi.is_forced(), mi.is_invalid()
    (True, False, False)
    >>> mi.make_forced()
    >>> mi.is_imported(), mi.is_forced(), mi.is_invalid()
    (False, True, False)
    >>> mi.make_invalid()
    >>> mi.is_imported(), mi.is_forced(), mi.is_invalid()
    (False, False, True)

    If we register a listener, it will be notified when the status of the
    entity changed:

    >>> ml = []
    >>> listener = lambda x: ml.append(x)
    >>> mi = MockImportable().register_listener(listener)
    >>> ml
    []
    >>> mi.make_imported()
    >>> ml # doctest: +ELLIPSIS
    [<MockImportable ...>]

    The listener should be notified **every time** the status changes:

    >>> ml = []
    >>> listener = lambda x: ml.append(x)
    >>> mi = MockImportable().register_listener(listener)
    >>> ml
    []
    >>> mi.make_imported()
    >>> mi.make_forced()
    >>> ml # doctest: +ELLIPSIS
    [<MockImportable ...>, <MockImportable ...>]

    It's up to the :py:class:`Importable` to define what a change is, the
    default implementation doesn't trigger a change when changing the status if
    the value was already the same.

    Registering the same listener multiple times shouldn't result in multiple
    calls when a change happens:

    >>> mi = MockImportable()
    >>> ml = []
    >>> listener = lambda x: ml.append(x)
    >>> mi.register_listener(listener) # doctest: +ELLIPSIS
    <MockImportable ...>
    >>> mi.register_listener(listener) # doctest: +ELLIPSIS
    <MockImportable ...>
    >>> mi.make_invalid()
    >>> ml # doctest: +ELLIPSIS
    [<MockImportable ...>]

    """
    __metaclass__ = abc.ABCMeta
    __slots__ = ('_status', 'listeners')

    def __init__(self):
        self._status = 0  # not set
        self.listeners = set()
        super(Importable, self).__init__()

    def register_listener(self, listener):
        """Register a callable  to be notified on changes.

        Registering the same listener multiple times shoudn't result in
        multiple nottifications to the same listener on a change.

        Return an :py:class:`Importable` instance equivalent to this one that
        has the listener registered. Can return the same instance.

        """
        assert callable(listener)
        self.listeners.add(listener)
        return self

    def register_change(self):
        """Notify the listeners that a change has occurred."""
        for listener in self.listeners:
            listener(self)

    def make_imported(self):
        """Mark as Imported_."""
        if self._status == 1:
            return
        self._status = 1
        self.register_change()

    def make_forced(self):
        """Mark as Forced_."""
        if self._status == 2:
            return
        self._status = 2
        self.register_change()

    def make_invalid(self):
        """Mark as Invalid_."""
        if self._status == 3:
            return
        self._status = 3
        self.register_change()

    def is_imported(self):
        """Check if is Imported_."""
        return self._status == 1

    def is_forced(self):
        """Check if is Forced_."""
        return self._status == 2

    def is_invalid(self):
        """Check if is Invalid_."""
        return self._status == 3

    @abc.abstractmethod
    def __hash__(self):
        """An **abstract method** that must compute the element hash value.

        Two equal :py:class:`Importable` instances must have equal hash values.

        """

    @abc.abstractmethod
    def __cmp__(self, other):
        """An **abstract method** that must check if two elements are the same.

        Two :py:class:`Importable` instances are equal if the *natural key* of
        both is equal.

        """
