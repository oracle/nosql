#
# json double field, vs int key
#
declare $k2 integer; // 3
select id
from foo f
where (f.info.bar1, f.info.bar2) in ((6, 3.4), (7, $k2), (7, $k2), (6.0, 3.4))
