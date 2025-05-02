select *
from A a left outer join A.B b on a.ida = b.ida
         left outer join A.B.C c ON b.ida = c.ida and b.idb = c.idb
         left outer join A.B.C.D d ON c.ida = d.ida and c.idb = d.idb and 
                                      c.idc = d.idc and c.c2 < 10
order by a.ida
