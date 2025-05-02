select c.ida as c_ida, c.idb as c_idb, c.idc as c_idc, a2
from nested tables(A.B.C c ancestors(A))
order by c.ida desc
