#
# typed int field, vs double key
#
select id
from foo f
where foo1 in (6, 3.0)
