# select all
select a.ida1, b.idb1, b.idb2, c.idc1, c.idc2, c3 from nested tables (A.B.C c ancestors (A a, A.B b) descendants (A.B.C.E e))
