select a.ida as a_ida, a.a1,
       b.ida as b_ida, b.idb as b_idb, b.b1, b.c1,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc, c.c2,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd, d.d1
from nested tables(A a descendants(A.B b, A.B.C c ON c.c2 < 10, A.B.C.D d))
order by a.ida
