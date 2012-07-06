Importable Elements
===================

.. py:module:: importtools.importables

This library defines an :py:class:`Importable` ``abc`` that represents elements
which can be imported.

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
title and counter can be detected and carried across systems thus keeping
elements which are the same but belong to different systems in sync.
Changes to the video URL will make the video element lose any corresponding
elements from other systems.

.. autoclass:: Importable
   :members:
   :undoc-members:

   .. automethod:: __hash__
   .. automethod:: __cmp__
