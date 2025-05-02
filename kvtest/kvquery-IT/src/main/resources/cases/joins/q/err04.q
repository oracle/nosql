select a.ida as a_ida, c1,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd
from nested tables(A a descendants(A.B.C.D d, A.G g))
order by a.ida
