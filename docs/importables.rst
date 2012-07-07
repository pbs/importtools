Importable Elements
===================

.. module:: importtools.importables

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
title and view counter can be detected and carried across systems thus keeping
elements which are the same in sync. Changes to the video URL will make the
video element lose any correspondence with elements belonging to other systems.

.. autoclass:: Importable

  .. automethod:: __init__
  .. automethod:: update
  .. automethod:: register
  .. automethod:: is_registered

.. autoclass:: RecordingImportable
  :show-inheritance:

  .. automethod:: changed
  .. automethod:: new
  .. automethod:: forget
