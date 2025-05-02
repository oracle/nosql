select a.ida as a_ida,
       b.ida as b_ida, b.idb as b_idb,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc
from nested tables(A a descendants(A.B.C c, A.B b))
where a.ida != 40
order by a.ida
