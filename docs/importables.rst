Importable Elements
===================
.. py:currentmodule:: importtools.importables

This library defines an :py:class:`Importable` abstraction that represents
elements that can be imported. This type of elements have 2 components:

* An immutable set of values that uniquely identify an element named
  *natural key*.
* A mutable set of properties.

A change to the natural key should be represented as a deletion of the
existing element and an addition of a new one.

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
