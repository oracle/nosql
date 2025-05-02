select d.ida as d_ida, d.idb as d_idb, count(*) as cnt
from A.B.C.D d left outer join A a on d.ida = a.ida
               left outer join A.B b on d.ida = b.ida and d.idb = b.idb
               left outer join A.B.C c on d.ida = c.ida and d.idb = c.idb and 
                                          d.idc = c.idc and c.c1 >= 15
where d.ida = 40 and d.idb > 0 and d.idd > 5
group by d.ida, d.idb
