#
# typed int field, vs json null key
#
select id
from foo f
where foo1 in (6, null)
