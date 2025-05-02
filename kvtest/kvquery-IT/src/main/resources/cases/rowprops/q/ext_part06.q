declare $p1 integer; // 3
select id
from foo $f
where $p1 <= partition($f) and partition($f) < 7
