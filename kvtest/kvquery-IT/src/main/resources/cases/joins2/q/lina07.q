select d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd,
       a.ida as a_ida,
       b.ida as b_ida, b.idb as b_idb,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc
from nested tables(A.B.C.D d ancestors(A a, A.B b, A.B.C c on c.c1 >= 15))
where b.idb > 0
order by d.ida
