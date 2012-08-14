"""This module contains a Django ORM specific loader.

It can be used to load a bunch of ordered rows directly from the DB using a
buffer. The resulting row data can be than used to generate Importable
instances used to create ``DataSet`` instances.

"""

import operator

from django.db.models import Q

from importtools import RecordingDataSet


class DjangoLoader(object):
    def __init__(self, Model, natural_key_attrs, content_attrs):
        self._model = Model
        self._natural_key_attrs = natural_key_attrs
        self._content_attrs = content_attrs

    def _get_basic_q(self):
        all_fields = list(self._content_attrs) + list(self._natural_key_attrs)
        q = self._model.objects.all().select_for_update()
        q = q.values(**all_fields)
        return q

    def _yield_from_q(self, q):
        for row in q:
            natural_key = []
            for attr_name in self._natural_key_attrs:
                natural_key.append(row[attr_name])
            content = {}
            for attr_name in self._content_attrs:
                content[attr_name] = row[attr_name]
            yield (tuple(natural_key), content), row

    def _make_gt(self, key, value):
        return Q(**{'%s__gt' % key: value})

    def _make_exact(self, key, value):
        return Q(**{'%s__exact' % key: value})

    def _make_cond(self, last_row):
        natural_keys = list(self._natural_key_attrs)

        all_partials = []

        while natural_keys:
            last_key = natural_keys[-1]
            partial_filter = gt = self._make_gt(last_key, last_row[last_key])
            natural_keys = natural_keys[:-1]
            if len(natural_keys) > 0:
                exact = []
                for key in natural_keys[:-1]:
                    exact.append(self._make_exact(key, last_row[key]))
                partial_filter = reduce(operator.and_, exact + [gt])
            all_partials.append(partial_filter)

        if len(all_partials) == 1:
            return all_partials[0]

        return reduce(operator.or_, all_partials)

    def load_all(self):
        for row_data, row in self._yield_from_q(self._get_basic_q()):
            yield row_data

    def load_buffered(self, buffer_size=16384):
        buffer_size = int(buffer_size)
        if buffer_size <= 0:
            raise ValueError("Buffer size must be positive.")

        base_q = self._get_basic_q()
        base_q = base_q.order_by(*self._natural_key_attrs)
        first_q = base_q[:buffer_size]

        count = -1
        for count, (row_data, row) in enumerate(self._yield_from_q(first_q)):
            last_row = row
            yield row_data

        while count == buffer_size:
            q = base_q.filter(self._make_cond(last_row))[:buffer_size]
            count = -1
            for count, (row_data, row) in enumerate(self._yield_from_q(q)):
                last_row = row
                yield row_data


def django_chunked_mem_sync(source_loader,
                            Model, natural_key_attrs, ImportableFactory,
                            content_attrs=None,
                            DSFactory=RecordingDataSet,
                            hint=16384):

    from importtools import chunked_mem_sync

    content_attrs = (
        content_attrs if content_attrs is not None
        else ImportableFactory.__content_attrs__
    )
    loader = DjangoLoader(Model, natural_key_attrs, content_attrs)

    def dest_loader():
        for row_data in loader:
            natural_key, content_dict = row_data
            yield ImportableFactory(natural_key, **content_dict)

    for dest_ds in chunked_mem_sync(
        source_loader, dest_loader(), DSFactory=DSFactory, hint=hint
    ):
        yield dest_ds
