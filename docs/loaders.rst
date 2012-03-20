Data Loaders
============

.. py:module:: importtools.loaders

There are also some format specific loaders available available like
:py:func:`args_loader` that groups elements from a list in order to
construct instances and a :py:func:`csv_loader` that can be used to parse
CSV files.

.. autofunction:: args_loader

.. autofunction:: csv_loader

The library provides a special *loader* function that can be used to populate
in parallel two ``DataSet`` instances in order to optimize a large import into
multiple smaller ones.

.. autofunction:: chunked_loader
