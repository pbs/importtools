Data Sets
=========
.. py:currentmodule:: importtools.datasets

The library provides two dataset abstractions that can be used in the import
system to provide a source and a destination for the
:py:class:`~.importtools.importables.Importable` elements.  The first one is a
read-only :py:class:`RODataSet` that can be used as the source of the data and
a fully mutable :py:class:`DataSet` that can be used as the source or the
destination of the import.

.. autoclass:: RODataSet
   :members:
   :undoc-members:

   .. automethod:: __contains__
   .. automethod:: __iter__

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

There are also some format specific :py:class:`DataSet` implementations
available like :py:class:`ArgsDataSet` that groups elements from a list in
order to create :py:class:`~.importtools.importables.Importable` instances and
a :py:class:`CSVDataSet` that can be used to parse CSV files and construct
:py:class:`~.importtools.importables.Importable` instances.

.. autoclass:: ArgsDataSet
   :show-inheritance:

   .. automethod:: populate

.. autoclass:: CSVDataSet
   :show-inheritance:

   .. automethod:: populate
