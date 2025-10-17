select a.ida, b.idb, count(case
                           when b.idb is null then b.idb
                           when c.idc is null then c.idc
                           else 1
                           end) as cnt
from nested tables(A a descendants(A.B b, A.B.C c))
group by a.ida, b.idb
