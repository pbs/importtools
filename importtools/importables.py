import abc
from operator import attrgetter


__all__ = ['Importable']


class Importable(object):
    """An :py:mod:`abc` that represents an element that can be imported.

    This :py:mod:`abc` has two abstract methods that need to be overridden by
    subclasses:

    * :py:meth:`__hash__`
    * :py:meth:`__cmp__`

    When the mutable part of an :py:class:`Importable` changes all the
    registered listeners, if any, must be notified. See
    :py:meth:`register_listener` and :py:meth:`register_change`.

    Each :py:class:`Importable` must know how to generate a hash based on its
    own natural keys. They must also define the equality comparison.  Two
    :py:class:`Importable` instances are considered equal only if their natural
    keys are the same.

    The :py:meth:`update` method is used to check for metadata changes between
    two equal :py:class:`Importable` instances.

    To exemplify the default functionality we first need to create a class that
    override all abstract methods:

    >>> from importtools.importables import MockImportable

    If we register a listener, it will be notified when the metadata changes:

    >>> mi = MockImportable(id=1, a=1, b=2)
    >>> ml = []
    >>> listener = lambda x: ml.append(x)
    >>> mi = mi.register_listener(listener)
    >>> mi.register_change()
    >>> ml
    [<MI a 1, b 2>]

    It's up to the :py:class:`Importable` to define what a change is in its
    ``update`` method, the default implementation doesn't trigger a change if
    the values of the attributes returned by :py:meth:`attrs` are the same:

    >>> mi = MockImportable(id=1, a=1, b=2)
    >>> ml = []
    >>> listener = lambda x: ml.append(x)
    >>> mi = mi.register_listener(listener)
    >>> mi.update(MockImportable(id=1, a=1, b=2))
    >>> ml
    []

    In case the values aren't the same, the :py:class:`Importable` is updated
    and :py:meth:`register_change` is called:

    >>> mi = MockImportable(id=1, a=1, b=2)
    >>> ml = []
    >>> listener = lambda x: ml.append(x)
    >>> mi = mi.register_listener(listener)
    >>> mi.update(MockImportable(id=1, a=2, b=3))
    >>> ml
    [<MI a 2, b 3>]

    Special case for single mutable attr:

    >>> mi = MockImportable(id=1, a=1, b=2)
    >>> mi.attrs = lambda: ['a']
    >>> ml = []
    >>> listener = lambda x: ml.append(x)
    >>> mi = mi.register_listener(listener)
    >>> mi.update(MockImportable(id=1, a=2, b=3))
    >>> ml
    [<MI a 2, b 2>]

    """
    __metaclass__ = abc.ABCMeta
    __slots__ = ('listeners', )

    def __init__(self, *args, **kwargs):
        self.listeners = set()
        super(Importable, self).__init__(*args, **kwargs)

    def register_listener(self, listener):
        """Register a callable  to be notified on changes.

        Registering the same listener multiple times shouldn't result in
        multiple notifications to the same listener on a change.

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

    def attrs(self):
        """Return the metadata attributes. Defaults to the empty list."""
        return []

    def update(self, other):
        """
        Update the current importable if needed based on the list of attributes
        returned by :py:meth:`attrs`.

        If any change was done :py:meth:`register_change` is called.

        """
        attrs = self.attrs()
        if not attrs:
            return
        get_many = attrgetter(*attrs)
        get_one = lambda importable: [get_many(importable)]
        ag = get_many if len(attrs) > 1 else get_one
        other_attrs = ag(other)
        if ag(self) == other_attrs:
            return
        for attr_name, value in zip(attrs, other_attrs):
            setattr(self, attr_name, value)
        self.register_change()

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


class MockImportable(Importable):
    def __init__(self, id, a, b):
        self.id = int(id)
        self.a = a
        self.b = b
        super(MockImportable, self).__init__()

    def attrs(self):
        return ['a', 'b']

    def __hash__(self):
        return self.id

    def __cmp__(self, other):
        return cmp(self.id, other.id)

    def __repr__(self):
        return '<MI a %s, b %s>' % (self.a, self.b)
