select c.ida as c_ida, c.idb as c_idb, c.idc as c_idc, a.ida as a_ida, a.a2
from nested tables(A.B.C c ancestors(A a))
order by c.ida
