select d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd,
       a.ida as a_ida,
       b.ida as b_ida, b.idb as b_idb,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc,
       d.ida + b.c1 as sum1,
       d.idb + c.ida as sum2,
       d.d2 + a.c1 as sum3
from nested tables(A.B.C.D d ancestors(A a, A.B b, A.B.C c))
where d.d2 > 15 and d.c3 < 100
