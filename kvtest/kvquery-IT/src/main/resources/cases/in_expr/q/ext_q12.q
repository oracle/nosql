#
# typed int field, vs EMPTY key
#
declare $k1 integer; // 6
select id
from foo f
where foo1 in (6, $k1[3])
