select a.ida, b.idb, count(*) as cnt
from nested tables(A a descendants(A.B b, A.B.C c))
group by a.ida, b.idb
