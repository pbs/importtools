Import Strategies
=================
.. py:currentmodule:: importtools.sync

There are multiple import strategies implemented in this library.  The
different startegies can take into account the states in which an
:py:class:`~.importtools.importables.Importable` can be in: ``Imported``,
``Forced`` and ``Invalid``.

.. autoclass:: FullSync
   :members:

.. autoclass:: AdditiveSync
   :members:

The library provides a special :py:class:`~.importtools.datasets.DataSet`
loader that can be used to populate in parallel two ``DataSet`` instances in
order to optimize a large import into multiple smaller ones.

.. autoclass:: ChunkedLoader
   :members:
