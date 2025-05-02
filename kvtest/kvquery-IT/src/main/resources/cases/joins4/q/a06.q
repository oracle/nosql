# on clause + exists operator + limit + offset
select c.ida1, c.idb1, c.idb2, b3, b4, idc1, idc2, c3 from nested tables (A.B.C c ancestors(A.B b on not exists b.b4.extra)) order by c.ida1, c.idb1, c.idb2, c.idc1 limit 4 offset 3
