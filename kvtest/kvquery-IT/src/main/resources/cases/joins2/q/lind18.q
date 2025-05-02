select a.ida as a_ida, a.c1 as a_c1,
       b.ida as b_ida, b.idb as b_idb, b.c1 as b_c1,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc, c.c1,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd, d.d1
from nested tables(A a 
                   descendants(A.B b ON b.b1 is null or b.c1 = a.c1,
                               A.B.C c,
                               A.B.C.D d))

