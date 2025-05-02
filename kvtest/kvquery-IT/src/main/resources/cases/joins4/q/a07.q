# map-filter step expressions
select c.ida1, c.idb1, c.idb2, b3, b4, idc1, idc2, c.c3.keys($value > 9000) as ckeys
from nested tables (A.B.C c ancestors(A.B b))
where c.idb2 = 2147483647 or c.idb1 = -2147483648
