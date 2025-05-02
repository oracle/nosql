# ON clause + multi-key index idx_c_c3 on A.B.C(c3.keys(), c3.values())
select c.ida1, c.idb1, c.idb2, c.idc1, c.idc2,
       c.c3.keys() as keys, c.c3.values() as values
from A.B.C c left outer join A.B b on c.ida1 = b.ida1 and c.idb1 = b.idb1 and
                                      c.idb2 = b.idb2 and b.idb2 > 0
where c.c3.ckey1 > 9002
