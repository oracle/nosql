select a.ida as a_ida,
       b.ida as b_ida, b.idb as b_idb,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd
from nested tables(A a descendants(A.B.C.D d, A.B b))
where a.ida != 40
order by a.ida
