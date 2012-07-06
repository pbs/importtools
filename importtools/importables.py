import abc
from operator import attrgetter


__all__ = ['Importable', 'RecordingImportable']


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

    The default :py:meth:`attrs` implementation returns an empty list of
    mutable fields and because of that :py:meth:`update` doesn't detect
    any changes:

    >>> class SimpleImportable(Importable):
    ...     def __init__(self, id, a):
    ...         self.id = id
    ...         self.a = a
    ...         super(SimpleImportable, self).__init__()
    ...     def __hash__(self): return self.id
    ...     def __cmp__(self): return cmp(self.id, other.id)
    ...     def __repr__(self): return '<SI a %s>' % self.a

    >>> mi = SimpleImportable(id=1, a=1)
    >>> ml = []
    >>> listener = lambda x: ml.append(x)
    >>> mi = mi.register_listener(listener)
    >>> mi.update(SimpleImportable(id=1, a=2))
    >>> ml
    []

    """
    __slots__ = ('listeners', '_natural_key')
    content_attrs = []

    _sentinel = object()

    def __init__(self, natural_key, *args, **kwargs):
        self.listeners = set()
        self._natural_key = natural_key
        super(Importable, self).__init__(*args, **kwargs)

    def is_registered(self, listener):
        """Checks if the listener is already registered."""
        return listener in self.listeners

    def register(self, listener):
        """Register a callable to be notified when an update changes data."""
        if not callable(listener):
            raise ValueError('Listener is not callable: %s' % listener)
        self.listeners.add(listener)
        return self

    def _notify(self):
        """Notify all listeners that an update has changed data."""
        for listener in self.listeners:
            listener(self)

    def update(self, other):
        """
        Add docs
        """
        sentinel = self._sentinel
        changed = False
        attr_iter = iter(self.content_attrs)
        for attr in attr_iter:
            this = getattr(self, attr, sentinel)
            that = getattr(other, attr, sentinel)
            if this is sentinel or that is sentinel:
                continue
            if not this == that:
                setattr(self, attr, that)
                changed = True
                break
        for attr in attr_iter:
            other_value = getattr(other, attr, sentinel)
            if other_value is not sentinel:
                setattr(self, attr, other_value)
        if changed:
            self._notify()

    def __hash__(self):
        return hash(self._natural_key)

    def __cmp__(self, other):
        return cmp(
            self._natural_key,
            other._natural_key
        )


class RecordingImportable(Importable):
    __slots__ = ('original_values', )

    def __init__(self, *args, **kwargs):
        self.original_values = dict()
        super(RecordingImportable, self).__init__(*args, **kwargs)

    def update(self, other):
        """
        Add docs here

        """
        sentinel = self._sentinel
        changed = False
        attr_iter = iter(self.content_attrs)
        for attr in attr_iter:
            this = getattr(self, attr, sentinel)
            that = getattr(other, attr, sentinel)
            if this is sentinel or that is sentinel:
                continue
            if not this == that:
                setattr(self, attr, that)
                if attr not in self.original_values:
                    self.original_values[attr] = this
                changed = True
        if changed:
            self.notify()

    def forget_changes(self):
        self.original_values = {}

    def has_changed(self, attr):
        return attr in self.original_values

    def original_value(self, attr):
        try:
            return self.original_values[attr]
        except KeyError:
            return getattr(self, attr)
