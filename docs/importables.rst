Importable Elements
===================

.. py:module:: importtools.importables

This library defines an :py:class:`Importable` abstraction that represents
elements that can be imported.

An ``Importable`` is usually composed of two different parts:

* An immutable *natural key* used to identify *the same* element in different
  datasets. This is the only required component for an :py:class:`Importable`.

  The natural key can be composed from multiple values but neither of them can
  change.

  Two elements that have the same natural key are said to be **the same**.

* An `Importable` element can also contain a set of mutable
  properties that form *the contents*.

  Two elements that are the same and have equal contents are said to be **in
  sync**.

For example, a *video element* can pick the value of the streaming URL to be its
natural key since changing the value of the URL represents changing the
underlying binary video content and thus doesn't represent the same video
anymore.  The mutable part can be composed from the video number of views, a
property that can change over time without changing the identity of the object
and should be carried across by the import algorithm.

.. autoclass:: Importable
   :members:
   :undoc-members:

   .. automethod:: __hash__
   .. automethod:: __cmp__
