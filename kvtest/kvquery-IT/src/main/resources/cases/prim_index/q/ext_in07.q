declare $x1 integer; // 3
        $y2 double;  // 30.0
        $z1 string;  // "tok0"
select id1, id2, id3
from foo
where (id1, id2) in (($x1, $y2), (4, 42.0), (1, 12.0)) and id3 > $z1
