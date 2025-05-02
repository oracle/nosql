select b.ida as b_ida, b.idb as b_idb,
        a.ida as a_ida, a.a2,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd
from nested tables(A.B b ancestors(A a) descendants(A.B.C.D d))
order by b.ida, b.idb
