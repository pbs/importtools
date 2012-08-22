from django.test import TestCase

from importtools.django_tests.models import TestModel


class TestLoading(TestCase):
    def setUp(self):
        for c in range(100):
            TestModel.objects.create(
                a=c / 10,
                b='b %s' % c,
                x=bool(c % 2),
                y='y %s' % c,
            )

    def tearDown(self):
        TestModel.objects.all().delete()

    def _get_target(self):
        from importtools import DjangoLoader
        return DjangoLoader

    def _make_one(self):
        return self._get_target()(TestModel, ['a', 'b'], ['x', 'y'])

    def test_load_all(self):
        l = self._make_one()
        row_data = l.load_all()
        r = list()
        for natural_key, content in row_data:
            r.append((natural_key, content))
        r = sorted(r)
        self._assert(r)

    def test_pozitive_buffer_size(self):
        l = self._make_one()
        read_one = lambda: l.load_buffered(buffer_size=-16).next()
        self.assertRaises(ValueError, read_one)

    def test_load_buffered(self):
        l = self._make_one()
        row_data = l.load_buffered(buffer_size=16)
        r = list()
        for natural_key, content in row_data:
            r.append((natural_key, content))
        self._assert(r)

    def _assert(self, r):
        first, second, third, last = r[0], r[1], r[2], r[-1]

        self.assertEqual(len(r), 100)
        natural_key, content = first
        self.assertEqual(natural_key, (0, 'b 0'))
        self.assertEqual(content, {'x': False, 'y': 'y 0'})
        natural_key, content = second
        self.assertEqual(natural_key, (0, 'b 1'))
        self.assertEqual(content, {'x': True, 'y': 'y 1'})
        natural_key, content = third
        self.assertEqual(natural_key, (0, 'b 2'))
        self.assertEqual(content, {'x': False, 'y': 'y 2'})
        natural_key, content = last
        self.assertEqual(natural_key, (9, 'b 99'))
        self.assertEqual(content, {'x': True, 'y': 'y 99'})
