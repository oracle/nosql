select d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd,
       a.ida as a_ida,
       b.ida as b_ida, b.idb as b_idb,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc
from A.B.C.D d left outer join A a on d.ida = a.ida
               left outer join A.B b on d.ida = b.ida and d.idb = b.idb
               left outer join A.B.C c on d.ida = c.ida and d.idb = c.idb and 
                                          d.idc = c.idc and c.c1 >= 15
where b.c1 > 0 and d.ida = 40 and d.idc > 0 and d.d1 > 5
order by d.ida
