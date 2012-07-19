DataSet Containers
==================

.. automodule:: importtools.datasets

.. autoclass:: DataSet

  .. automethod:: __iter__
  .. automethod:: get
  .. automethod:: add
  .. automethod:: pop
  .. automethod:: sync

.. autoclass:: SimpleDataSet
  :show-inheritance:

.. autoclass:: RecordingDataSet
  :show-inheritance:

  .. automethod:: reset
  .. autoattribute:: added
  .. autoattribute:: removed
  .. autoattribute:: changed
