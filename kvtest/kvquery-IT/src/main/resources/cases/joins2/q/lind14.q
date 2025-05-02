select b.ida as b_ida, b.idb as b_idb,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd
from nested tables(A.B b descendants(A.B.C.D d, A.B.C c))
where b.ida != 40
order by b.ida, b.idb
