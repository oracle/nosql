declare $p4 integer; // 4
        $p5 integer; // 5
select id
from foo $f
where $p5 <= partition($f) and partition($f) < $p4
