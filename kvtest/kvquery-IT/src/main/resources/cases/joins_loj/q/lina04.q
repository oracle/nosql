select d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd,
       b.ida as b_ida, b.idb as b_idb, b.c1 as b_c1,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc
from A.B.C.D d left outer join A.B b on d.ida = b.ida and d.idb = b.idb
               left outer join A.B.C c on d.ida = c.ida and d.idb = c.idb and
                                          d.idc = c.idc
order by d.ida
