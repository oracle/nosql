#
# typed int field, vs long key
#
select id
from foo f
where foo1 in (6, cast(4 as long))
