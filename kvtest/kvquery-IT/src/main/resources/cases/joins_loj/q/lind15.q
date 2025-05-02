select *
from A a left outer join A.B b on a.ida = b.ida
         left outer join A.B.C c ON b.ida = c.ida and b.idb = c.idb and 
                                    c.c2 < 10
         left outer join A.B.C.D d on c.ida = d.ida and c.idb = d.idb and 
                                      c.idc = d.idc 
order by a.ida
