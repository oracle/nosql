#
# json int field, vs json null key
#
declare $k10 integer; // null
select id
from foo f
where f.info.bar1 in (6, $k10)
