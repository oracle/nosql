select c.ida as c_ida, c.idb as c_idb, c.idc as c_idc, a.ida as a_ida, a.a2
from A.B.C c left outer join A a on c.ida = a.ida
order by c.ida
