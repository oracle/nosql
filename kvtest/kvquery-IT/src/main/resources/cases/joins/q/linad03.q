select c.ida as c_ida, c.idb as c_idb, c.idc as c_idc,
       a.ida as a_ida, a.a2,
       b.ida as b_ida, b.idb as b_idb,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd
from nested tables(A.B.C c ancestors(A a, A.B b)
                           descendants(A.B.C.D d on b.b2 = 45 or
                                                    b.b2 is null and d.c3 < 32))
order by c.ida, c.idb
