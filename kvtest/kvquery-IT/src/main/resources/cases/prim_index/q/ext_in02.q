declare $x1 integer; // 3
        $x2 integer; // 4
        $z1 string;  // "tok0"
        $w1 string; // "id4-3"
select id1, id2, id3, id4
from foo
where (id1, id3, id4) in (($x1, $z1, $w1), ($x2, "tok1", "id4-4"))
