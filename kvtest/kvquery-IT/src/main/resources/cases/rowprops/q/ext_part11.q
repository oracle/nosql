declare $p6 integer; // 0
select id
from foo $f
where $p6 <= partition($f) and partition($f) < 4
