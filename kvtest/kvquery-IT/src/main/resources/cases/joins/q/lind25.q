select /* FORCE_PRIMARY_INDEX(A) */
       a.ida, b.ida, b.idb
from nested tables(A a descendants(A.B b))
order by a.ida, b.ida, b.idb
limit 10
