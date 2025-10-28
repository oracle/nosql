select a.ida, b.idb, c.idc
from nested tables(A a descendants(A.B b on b.idb = 0, A.B.C c))
order by a.ida, b.idb, c.idc
