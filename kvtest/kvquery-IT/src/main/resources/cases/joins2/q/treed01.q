select a.ida as a_ida,
       b.ida as b_ida, b.idb as b_idb,
       g.ida as g_ida, g.idg as g_idg
from nested tables(A a descendants(A.B b, A.G g))
order by a.ida
