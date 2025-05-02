select a.ida as a_ida,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc
from A a left outer join A.B.C c on a.ida = c.ida
where a.ida != 40
order by a.ida
