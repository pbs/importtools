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

    ``Importable`` instances are hashable and comparable based on the natural
    key values:

    >>> i1 = Importable(0)
    >>> i2 = Importable(0)
    >>> hash(i1) == hash(i2)
    True
    >>> i1 == i2
    True
    >>> not i1 < i2
    True

    """

    __slots__ = ('_listeners', '_natural_key')
    content_attrs = []

    def __init__(self, natural_key, *args, **kwargs):
        """Create a new ``Importable`` using the *natural key* provided.

        The natural key must be hashable and should implement equality and less
        then operators.

        ``Importable`` elements can access the *natural_key* value used on
        instantiation using the ``natural_key`` property:

        >>> i = Importable((123, 'abc'))
        >>> i.natural_key
        (123, 'abc')

        """
        self._listeners = set()
        self._natural_key = natural_key
        super(Importable, self).__init__(*args, **kwargs)

    @property
    def natural_key(self):
        return self._natural_key

    def update(self, other):
        """Puts this element in sync with the *other*.

        The default implementation uses ``content_attrs`` to search for
        the attributes that need to be synced between the elements and it
        copies the values of each attribute it finds from the *other* element
        in this one.

        By default the ``self.content_attrs`` is an empty list so no
        synchronization will take place:

        >>> class MockImportable(Importable): pass
        >>> i1 = MockImportable(0)
        >>> i2 = MockImportable(0)
        >>> i1.a, i1.b = 'a1', 'b1'
        >>> i2.a, i2.b = 'a2', 'b2'

        >>> has_changed = i1.update(i2)
        >>> i1.a
        'a1'

        >>> i1.content_attrs = ['a', 'b', 'x']
        >>> has_changed = i1.update(i2)
        >>> i1.a, i1.b
        ('a2', 'b2')

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
        >>> has_changed = i1.update(i2)
        >>> len(notifications)
        1
        >>> notifications[0] is i1
        True

        All attributes that can't be found in the *other* element are skipped:

        >>> i1.content_attrs = ['a', 'b', 'c']
        >>> has_changed = i1.update(i2)
        >>> hasattr(i1, 'c')
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
            if this != that:  # sentinel will also be different
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
        """Register a callable to be notified when ``update`` changes data.

        This method should raise an ``ValueError`` if *listener* is not a
        callable:

        >>> i = Importable(0)
        >>> i.register(1) # doctest:+IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ValueError:

        """
        if not callable(listener):
            raise ValueError('Listener is not callable: %s' % listener)
        self._listeners.add(listener)

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
        return listener in self._listeners

    def _notify(self):
        for listener in self._listeners:
            listener(self)

    def __hash__(self):
        return hash(self._natural_key)

    def __eq__(self, other):
        return self._natural_key == other._natural_key

    def __lt__(self, other):
        return self._natural_key < other._natural_key

    def __repr__(self):
        """
        >>> Importable((1, 'a'))
        Importable((1, 'a'))

        >>> class TestImportable(Importable): pass
        >>> TestImportable('xyz')
        TestImportable('xyz')

        """
        cls_name = self.__class__.__name__
        return '%s(%r)' % (cls_name, self._natural_key)


class RecordingImportable(Importable):
    """Very similar to :py:class:`Importable` but tracks changes.

    This class records the original values that the attributes had before
    :py:meth:``update`` synced this element with another one.

    """
    __slots__ = ('_original_values', '_new_attributes')

    def __init__(self, *args, **kwargs):
        self._original_values = dict()
        self._new_attributes = set()
        super(RecordingImportable, self).__init__(*args, **kwargs)

    def update(self, other):
        changed = False
        sentinel = object()
        attr_iter = iter(self.content_attrs)
        for attr in attr_iter:
            try:
                that = getattr(other, attr)
            except AttributeError:
                continue
            this = getattr(self, attr, sentinel)
            if this != that:  # sentinel will also be different
                setattr(self, attr, that)
                changed = True
                if this is not sentinel:
                    if attr not in self._original_values:
                        self._original_values[attr] = this
                else:
                    self._new_attributes.add(attr)
        if changed:
            self._notify()
        return changed

    def changed(self):
        """Return a dictionary with all attributes that were changed.

        The dit keys are attribute names and the values are equal to what the
        attributes contained before the sync done by :py:meth:``update``.

        >>> class MockImportable(RecordingImportable):
        ...   content_attrs = ['a']
        >>> i1 = MockImportable(0)
        >>> i1.a = 'a1'
        >>> i2 = MockImportable(0)
        >>> i2.a = 'a2'

        >>> i1.changed()
        {}
        >>> has_changed = i1.update(i2)
        >>> i1.a
        'a2'
        >>> i1.changed()
        {'a': 'a1'}

        """
        return dict(self._original_values)

    def new(self):
        """Return a set of attribute names that were created.

        This attribute were not set on the element before the sync done by
        :py:meth:``update`` so they don't have an original value and thus are
        not available in :py:meth:``changed``.

        >>> class MockImportable(RecordingImportable):
        ...   content_attrs = ['a', 'b']
        >>> i1 = MockImportable(0)
        >>> i1.a = 'a1'
        >>> i2 = MockImportable(0)
        >>> i2.b = 'b2'

        >>> i1.new()
        set([])
        >>> has_changed = i1.update(i2)
        >>> i1.b
        'b2'
        >>> i1.new()
        set(['b'])

        """
        return set(self._new_attributes)

    def forget(self):
        """Forget all memorized changes.

        Calling this method will empty out ``changed`` and ``new``:

        >>> class MockImportable(RecordingImportable):
        ...   content_attrs = ['a', 'b']
        >>> i1 = MockImportable(0)
        >>> i1.a = 'a1'
        >>> i2 = MockImportable(0)
        >>> i2.a = 'a2'
        >>> i2.b = 'b2'
        >>> has_changed = i1.update(i2)

        >>> i1.changed()
        {'a': 'a1'}
        >>> i1.new()
        set(['b'])
        >>> i1.forget()
        >>> i1.changed()
        {}
        >>> i1.new()
        set([])

        """
        self._original_values = {}
        self._new_attributes = set()
