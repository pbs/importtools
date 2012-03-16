Data Sets
=========
.. py:currentmodule:: importtools.datasets

The library provides a :py:class:`DataSet` abstractions that can be used in the import
system to provide a source and a destination for the
:py:class:`~.importtools.importables.Importable` elements.  

.. autoclass:: DataSet
   :members:
   :undoc-members:
   :show-inheritance:

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
