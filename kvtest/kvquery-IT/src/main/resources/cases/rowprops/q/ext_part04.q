declare $p1 integer; // 3
select id
from foo $f
where partition($f) = $p1 and id = 1
