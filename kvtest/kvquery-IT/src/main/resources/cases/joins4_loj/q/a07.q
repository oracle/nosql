# map-filter step expressions
select c.ida1, c.idb1, c.idb2, b3, b4, idc1, idc2,
       c.c3.keys($value > 9000) as ckeys
from A.B.C c left outer join A.B b on c.ida1 = b.ida1 and c.idb1 = b.idb1 and
                                      c.idb2 = b.idb2
where c.idb2 = 2147483647 or c.idb1 = -2147483648
