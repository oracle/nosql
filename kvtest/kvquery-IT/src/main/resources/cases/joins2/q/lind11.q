select a.ida as a_ida,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd
from nested tables(A a descendants(A.B.C.D d))
where a.ida != 40
order by a.ida
