declare $x1 integer; // 3
        $y1 double;  // 42.0
select *
from foo
where (id1, id2) in (($x1, 30.0), (4, $y1))
