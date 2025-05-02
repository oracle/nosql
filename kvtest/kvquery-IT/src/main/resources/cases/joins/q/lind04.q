select $a, b.ida, b.idb, $c.ida, $c.idb, $c.idc, d.ida, d.idb, d.idc, d.idd
from nested tables(A $a descendants(A.B b, A.B.C $c, A.B.C.D d))
where $a.ida = 0
