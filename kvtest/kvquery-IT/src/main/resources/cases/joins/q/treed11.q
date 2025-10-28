select /* FORCE_PRIMARY_INDEX(A) */
       a.ida, b.ida, b.idb, g.idg
from nested tables(A a descendants(A.B b, A.G g))
order by a.ida, b.idb
