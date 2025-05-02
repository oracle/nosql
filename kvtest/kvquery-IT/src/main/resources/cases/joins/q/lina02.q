select c.ida as c_ida, c.idb as c_idb, c.idc as c_idc,
       a.ida as a_ida, a.c1 as a_c1,
       b.ida as b_ida, b.idb as b_idb, b.c1 as b_c1 
from nested tables(A.B.C c ancestors(A a, A.B b))
order by c.ida
