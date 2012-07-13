"""This module defines some handy :py:class:`Importable` elements.

An ``Importable`` is usually composed of two different parts:

* A *natural key* used to identify *the same* element across different systems.
  This is the only required component for an ``Importable``.

* An optional set of properties that form *the contents*. The data in this
  properties is carried across systems in the process of syncing the elements.

Two elements that are *the same* and have *equal contents* are said to be *in
sync*.

For example an element representing an online video can use the value of the
streaming URL to be its natural key. The contents of the element can be formed
from a view counter and the video title. In this scenario changes on the video
title and view counter can be detected and carried across systems thus keeping
elements which are the same in sync. Changes to the video URL will make the
video element lose any correspondence with elements belonging to other systems.

"""

__all__ = ['Importable', 'RecordingImportable']


class _AutoContent(type):

    def __new__(cls, name, bases, d):
        _magic_name = '__content_attrs__'

        if _magic_name not in d:
            return type.__new__(cls, name, bases, d)

        ca = d[_magic_name]
        # XXX: py3
        if isinstance(ca, basestring):
            raise ValueError(
                '%s must be an iterable not a string.' % _magic_name
            )

        try:
            ca = frozenset(ca)
        except TypeError:
            raise ValueError('%s must be iterable.' % _magic_name)

        def _init_(self, *args, **kwargs):
            update_kwargs = {}
            for content_attr in self._content_attrs:
                try:
                    update_kwargs[content_attr] = kwargs.pop(content_attr)
                except KeyError:
                    pass  # All arguments are optional
            self.update(**update_kwargs)
            super(klass, self).__init__(*args, **kwargs)

        d['__init__'] = _init_
        d['__slots__'] = frozenset(d.get('__slots__', [])) | ca
        d['_content_attrs'] = ca

        klass = type.__new__(cls, name, bases, d)
        return klass


class Importable(object):
    """A default implementation representing an importable element.

    This class is intended to be specialized in order to provide the element
    content and to override its behaviour if needed.

    The :py:meth:`sync` implementation in this class doesn't keep track of
    changed values. For such an implementation see
    :py:class:`RecordingImportable`.

    ``Importable`` instances are hashable and comparable based on the
    *natural_key* value. Because of this the *natural_key* must also be
    hashable and should implement equality and less then operators:

    >>> i1 = Importable(0)
    >>> i2 = Importable(0)
    >>> hash(i1) == hash(i2)
    True
    >>> i1 == i2
    True
    >>> not i1 < i2
    True

    ``Importable`` elements can access the *natural_key* value used on
    instantiation trough the ``natural_key`` property:

    >>> i = Importable((123, 'abc'))
    >>> i.natural_key
    (123, 'abc')

    Listeners can register to observe an ``Importable`` element for changes.
    Every time the content attributes change with a value that is not equal to
    the previous one all registered listeners will be notified:

    >>> class MockImportable(Importable):
    ...     _content_attrs = ['a', 'b']
    >>> i = MockImportable(0)

    >>> notifications = []
    >>> i.register(lambda x: notifications.append(x))
    >>> i.a = []
    >>> i.b = 'b'
    >>> i.b = 'bb'
    >>> len(notifications)
    3
    >>> notifications[0] is notifications[1] is notifications[2] is i
    True

    >>> notifications = []
    >>> l = []
    >>> i.a = l
    >>> len(notifications)
    0
    >>> i.a is l
    True

    """

    __metaclass__ = _AutoContent
    __slots__ = ('_listeners', '_natural_key')
    _content_attrs = frozenset([])

    def __init__(self, natural_key, *args, **kwargs):
        self._listeners = set()
        self._natural_key = natural_key
        super(Importable, self).__init__(*args, **kwargs)

    @property
    def natural_key(self):
        return self._natural_key

    def __setattr__(self, attr, value):
        is_different = False
        if attr in self._content_attrs:
            is_different = getattr(self, attr, object()) != value
        super(Importable, self).__setattr__(attr, value)
        if is_different:
            self._notify()

    def update(self, **kwargs):
        """Update multiple content attrtibutes and fire a single notification.

        Multiple changes to the element content can be grouped in a single call
        to :py:meth:`update`. This method should return ``True`` if at least
        one element differed from the original values or else ``False``.

        >>> class MockImportable(Importable):
        ...     _content_attrs = ['a', 'b']
        >>> i = MockImportable(0)
        >>> i.register(lambda x: notifications.append(x))

        >>> notifications = []
        >>> i.update(a=100, b=200)
        True
        >>> len(notifications)
        1
        >>> notifications[0] is i
        True
        >>> notifications = []
        >>> i.update(a=100, b=200)
        False
        >>> len(notifications)
        0

        """
        has_changed = False
        sentinel = object()
        super_ = super(Importable, self)
        content_attrs = self._content_attrs
        for attr_name, value in kwargs.items():
            if attr_name not in content_attrs:
                raise ValueError(
                    'Attribute %s is not part of the element content.'
                    % attr_name
                )
            if not has_changed:
                current_value = getattr(self, attr_name, sentinel)
                # Sentinel will also be different
                if current_value != value:
                    has_changed = True
            super_.__setattr__(attr_name, value)
        if has_changed:
            self._notify()
        return has_changed

    def sync(self, other):
        """Puts this element in sync with the *other*.

        The default implementation uses ``_content_attrs`` to search for
        the attributes that need to be synced between the elements and it
        copies the values of each attribute it finds from the *other* element
        in this one.

        By default the ``self._content_attrs`` is an empty list so no
        synchronization will take place:

        >>> class MockImportable(Importable):
        ...     pass
        >>> i1 = MockImportable(0)
        >>> i2 = MockImportable(0)

        >>> i1.a, i1.b = 'a1', 'b1'
        >>> i2.a, i2.b = 'a2', 'b2'

        >>> has_changed = i1.sync(i2)
        >>> i1.a
        'a1'

        >>> class MockImportable(Importable):
        ...     _content_attrs = ['a', 'b', 'x']
        >>> i1 = MockImportable(0)
        >>> i2 = MockImportable(0)
        >>> i1.a, i1.b = 'a1', 'b1'
        >>> i2.a, i2.b = 'a2', 'b2'

        >>> has_changed = i1.sync(i2)
        >>> i1.a, i1.b
        ('a2', 'b2')

        If no synchronization was needed (i.e. the content of the elements were
        equal) this method should return ``False``, otherwise it should return
        ``True``:

        >>> i1.sync(i2)
        False
        >>> i1.a = 'a1'
        >>> i1.sync(i2)
        True

        If the sync mutated this element all listeners should be notified. See
        :py:meth:`register`:

        >>> i1.a = 'a1'
        >>> notifications = []
        >>> i1.register(lambda x: notifications.append(x))
        >>> has_changed = i1.sync(i2)
        >>> len(notifications)
        1
        >>> notifications[0] is i1
        True

        All attributes that can't be found in the *other* element are skipped:

        >>> i1._content_attrs = ['a', 'b', 'c']
        >>> has_changed = i1.sync(i2)
        >>> hasattr(i1, 'c')
        False

        """
        return self._sync(self._content_attrs, other)

    def _sync(self, content_attrs, other):
        kwargs = {}
        for attr in content_attrs:
            try:
                that = getattr(other, attr)
            except AttributeError:
                continue
            else:
                kwargs[attr] = that
        return self.update(**kwargs)

    def register(self, listener):
        """Register a callable to be notified when ``sync`` changes data.

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
        """Sends a notification to all listeners passing this element."""
        for listener in self._listeners:
            listener(self)

    def __hash__(self):
        return hash(self._natural_key)

    def __eq__(self, other):
        return self._natural_key == other.natural_key

    def __lt__(self, other):
        return self._natural_key < other.natural_key

    def __repr__(self):
        """
        >>> Importable((1, 'a'))
        Importable((1, 'a'), ...)

        >>> class TestImportable(Importable): pass
        >>> TestImportable('xyz')
        TestImportable('xyz', ...)

        """
        cls_name = self.__class__.__name__
        return '%s(%r, ...)' % (cls_name, self._natural_key)


class _Original(Importable):
    # This class has __dict__ enabled so _content_attrs can be overriden for
    # each instance.
    def __init__(self, content_attrs, *args, **kwargs):
        self._content_attrs = content_attrs
        super(_Original, self).__init__(*args, **kwargs)


class RecordingImportable(Importable):
    """Very similar to :py:class:`Importable` but tracks changes.

    This class records the original values that the attributes had before
    any change introduced by attribute assignment or call to ``update`` and
    ``sync``.

    """

    __slots__ = ('_original', )

    def __init__(self, *args, **kwargs):
        super(RecordingImportable, self).__init__(*args, **kwargs)
        self.reset()

    @property
    def orig(self):
        """An object that can be used to access the elements original values.

        The object has all the attributes that this element had when it was
        instantiated or last time when :py:meth:`reset` was called.


        >>> class MockImportable(RecordingImportable):
        ...   _content_attrs = ['a']
        >>> i = MockImportable(0)

        >>> hasattr(i.orig, 'a')
        False
        >>> i.a = 'a'
        >>> i.reset()
        >>> i.a
        'a'
        >>> i.orig.a
        'a'
        >>> i.a = 'aa'
        >>> i.a
        'aa'
        >>> i.orig.a
        'a'

        """
        return self._original

    def reset(self):
        """Forget all memorized changes.

        >>> class MockImportable(RecordingImportable):
        ...   _content_attrs = ['a']
        >>> i = MockImportable(0)

        >>> hasattr(i.orig, 'a')
        False
        >>> i.a = 'a'
        >>> i.reset()
        >>> i.a = 'aa'
        >>> i.orig.a
        'a'
        >>> i.reset()
        >>> i.orig.a
        'aa'

        """
        # Create new instance since _content_attrs can shrunk in time and we
        # need not keep values that are not in that list anymore.
        self._original = _Original(self._content_attrs, self.natural_key)
        self._original.sync(self)
