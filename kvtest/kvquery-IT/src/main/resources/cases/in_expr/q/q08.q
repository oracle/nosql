#
# typed int field, vs string key
#
select id
from foo f
where foo1 in (6, "3")
