Data Sets
=========

.. py:module:: importtools.datasets

The library provides a :py:class:`DataSet` abstractions that can be used in the
import :py:class:`~.importtools.importables.Importable` elements.  The first
one is a read-only :py:class:`RODataSet` that can be used as the source of the
data and a fully mutable :py:class:`DataSet` that can be used as the source or
the destination of the import.

.. autoclass:: RODataSet
   :members:
   :undoc-members:

   .. automethod:: __contains__
   .. automethod:: __iter__

.. autoclass:: DataSet
   :members:
   :undoc-members:
   :show-inheritance:

   .. automethod:: __len__

There are two major implementations available: an in-memory simple
:py:mod:`dict`-based :py:class:`MemoryDataSet` implementation and a
:py:class:`DiffDataSet` implementation that acts as a :py:class:`DataSet`
wrapper that remembers all changes, additions and removals.

.. autoclass:: MemoryDataSet
   :show-inheritance:

.. autoclass:: DiffDataSet
   :show-inheritance:

   .. automethod:: register_change
   .. automethod:: get_added
   .. automethod:: get_removed
   .. automethod:: get_changed
