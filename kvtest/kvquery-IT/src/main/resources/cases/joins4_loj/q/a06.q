# on clause + exists operator + limit + offset
select c.ida1, c.idb1, c.idb2, b3, b4, idc1, idc2, c3
from A.B.C c left outer join A.B b on c.ida1 = b.ida1 and c.idb1 = b.idb1 and
                                      c.idb2 = b.idb2 and not exists b.b4.extra
order by c.ida1, c.idb1, c.idb2, c.idc1
limit 4 offset 3
