import pytest

from aiocarbon.protocol.base import strip_carbon_ns


EXPECTED = (
    ('((foo))', 'foo'),
    ('bar//\'*75$baz', 'bar_75_baz'),
    ('..', ''),
    ('foo.bar', 'foo.bar'),
    ('..foo.bar', 'foo.bar'),
    ('..foo.bar..', 'foo.bar'),
    ('..foo..bar..', 'foo_bar'),
)


@pytest.mark.parametrize("value,expected", EXPECTED)
def test_strip_carbon_ns(value, expected):
    assert strip_carbon_ns(value) == expected
