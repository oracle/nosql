# ON clause + multi-key index idx_c_c3 on A.B.C(c3.keys(), c3.values())
select c.ida1, c.idb1, c.idb2, c.idc1, c.idc2, c.c3.keys() as keys, c.c3.values() as values from nested tables (A.B.C c ancestors (A.B b on b.idb2 > 0 )) where c.c3.ckey1 > 9002
