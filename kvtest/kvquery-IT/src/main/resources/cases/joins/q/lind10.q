select a.ida as a_ida,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc
from nested tables(A a descendants(A.B.C c))
where a.ida != 40
order by a.ida
