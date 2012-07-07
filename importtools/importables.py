import abc
from operator import attrgetter


__all__ = ['Importable', 'RecordingImportable']


class Importable(object):
    """A default implementation representing an importable element.

    This class is intended to be specialized in order to provide the element
    content and to override its behaviour if needed.

    The :py:meth:`update` implementation in this class doesn't keep track of
    changed values. For such an implementation see
    :py:class:`RecordingImportable`.

    """

    __slots__ = ('listeners', '_natural_key')
    content_attrs = []

    def __init__(self, natural_key, *args, **kwargs):
        """Create a new ``Importable`` with the *natural key*.

        The natural key must be hashable and should implement equality and less
        then operators.

        """
        self.listeners = set()
        self._natural_key = natural_key
        super(Importable, self).__init__(*args, **kwargs)

    def update(self, other):
        """Puts this element in sync with the *other*.

        The default implementation uses ``content_attrs`` to search for
        the attributes that need to be synced between the elements and it
        copies the values of each attribute it finds from the *other* element
        in this one.

        By default the ``self.content_attrs`` is an empty list so no
        synchronization will take place:

        >>> class MockImportable(Importable): pass
        >>> i1 = MockImportable(1)
        >>> i2 = MockImportable(2)
        >>> i1.a = 'a1'
        >>> i2.a = 'a2'

        >>> status = i1.update(i2)
        >>> i1.a
        'a1'

        >>> i1.content_attrs = ['a']
        >>> status = i1.update(i2)
        >>> i1.a
        'a2'

        If no synchronization was needed (i.e. the content of the elements were
        equal) this method should return ``False``, otherwise it should return
        ``True``:

        >>> i1.update(i2)
        False
        >>> i1.a = 'a1'
        >>> i1.update(i2)
        True

        If the sync mutated this element all listeners should be notified. See
        :py:meth:`register`:

        >>> notifications = []
        >>> i1.register(lambda x: notifications.append(x))
        >>> i1.a = 'a1'
        >>> status = i1.update(i2)
        >>> len(notifications)
        1
        >>> notifications[0] is i1
        True

        All attributes that can't be found in the *other* element are skipped:

        >>> i1.content_attrs = ['a', 'b']
        >>> status = i1.update(i2)
        >>> hasattr(i1, 'b')
        False

        """
        changed = False
        sentinel = object()
        attr_iter = iter(self.content_attrs)
        for attr in attr_iter:
            try:
                that = getattr(other, attr)
            except AttributeError:
                continue
            this = getattr(self, attr, sentinel)
            if this is sentinel or this != that:
                setattr(self, attr, that)
                changed = True
                break
        for attr in attr_iter:
            try:
                other_value = getattr(other, attr)
            except AttributeError:
                continue
            setattr(self, attr, other_value)
        if changed:
            self._notify()
        return changed

    def register(self, listener):
        """
        Register a callable to be notified when :py:meth:`update` mutates data.

        This method should raise an ``ValueError`` if *listener* is not a
        callable:

        >>> i = Importable(0)
        >>> i.register(1) # doctest:+IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ValueError:

        """
        if not callable(listener):
            raise ValueError('Listener is not callable: %s' % listener)
        self.listeners.add(listener)

    def is_registered(self, listener):
        """Check if the listener is already registered.

        >>> i = Importable(0)
        >>> a = lambda x: None
        >>> i.is_registered(a)
        False
        >>> i.register(a)
        >>> i.is_registered(a)
        True

        """
        return listener in self.listeners

    def _notify(self):
        for listener in self.listeners:
            listener(self)

    def __hash__(self):
        return hash(self._natural_key)

    def __eq__(self, other):
        return self._natural_key == other._natural_key

    def __lt__(self, other):
        return self._natural_key < other._natural_key


class RecordingImportable(Importable):
    __slots__ = ('original_values', )

    def __init__(self, *args, **kwargs):
        self.original_values = dict()
        super(RecordingImportable, self).__init__(*args, **kwargs)

    def update(self, other):
        """
        Add docs here

        """
        changed = False
        attr_iter = iter(self.content_attrs)
        for attr in attr_iter:
            try:
                this = getattr(self, attr)
                that = getattr(other, attr)
            except AttributeError:
                continue
            if not this == that:
                setattr(self, attr, that)
                if attr not in self.original_values:
                    self.original_values[attr] = this
                changed = True
        if changed:
            self._notify()
        return changed

    def forget_changes(self):
        self.original_values = {}

    def has_changed(self, attr):
        return attr in self.original_values

    def original_value(self, attr):
        try:
            return self.original_values[attr]
        except KeyError:
            return getattr(self, attr)
