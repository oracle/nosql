#
# typed int field, vs EMPTY key
#
select id
from foo f
where foo1 in (6, seq_concat())
