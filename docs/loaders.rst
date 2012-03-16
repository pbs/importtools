Data Loaders
============
.. py:currentmodule:: importtools.loaders

There are also some format specific :py:class:`Loader` implementations
available like :py:class:`ArgsLoader` that groups elements from a list in
order to create :py:class:`~.importtools.importables.Importable` instances and
a :py:class:`CSVLoader` that can be used to parse CSV files and construct
:py:class:`~.importtools.importables.Importable` instances.

.. autoclass:: Loader
   :members:

   .. automethod:: __iter__

.. autoclass:: ArgsLoader
   :show-inheritance:
   :members:

   .. automethod:: __iter__

.. autoclass:: CSVLoader
   :show-inheritance:
   :members:

   .. automethod:: __iter__

The library provides a special *loader* that can be used to populate in
parallel two ``DataSet`` instances in order to optimize a large import into
multiple smaller ones.

.. autoclass:: ChunkedLoader
   :members:
