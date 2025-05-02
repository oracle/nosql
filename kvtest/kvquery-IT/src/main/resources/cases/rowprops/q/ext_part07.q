declare $p1 integer; // 3
        $p2 long; // 15
select id
from foo $f
where $p1 <= partition($f) and partition($f) < $p2

