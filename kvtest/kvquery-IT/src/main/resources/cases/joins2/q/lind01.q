select $a.ida as a_ida, remaining_days($a) as a_days,
       $b.ida as b_ida, $b.idb as b_idb, remaining_days($b) as b_days,
       $c.ida as c_ida, $c.idb as c_idb, $c.idc as c_idc, remaining_days($c) as c_days,
       $d.ida as d_ida, $d.idb as d_idb, $d.idc as d_idc, $d.idd as d_idd, d2
from nested tables(A $a descendants(A.B $b, A.B.C $c, A.B.C.D $d))
where $a.ida != 40
