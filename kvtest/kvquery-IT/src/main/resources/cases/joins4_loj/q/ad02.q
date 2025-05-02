# select all
select a.ida1, b.idb1, b.idb2, c.idc1, c.idc2, c3
from A.B.C c left outer join A a on c.ida1 = a.ida1
             left outer join A.B b on c.ida1 = b.ida1 and c.idb1 = b.idb1 and
                                      c.idb2 = b.idb2
             left outer join A.B.C.E e on c.ida1 = e.ida1 and
                      c.idb1 = e.idb1 and c.idb2 = e.idb2 and
                      c.idc1 = e.idc1 and c.idc2 = e.idc2
