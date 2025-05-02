#
# typed int field, vs long key: the long key cannot be removed because foo1 is nullable
#
select id
from foo f
where foo1 in (6, 8589934592)
