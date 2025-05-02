select b.ida as b_ida, b.idb as b_idb,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc
from A.B b left outer join A.B.C c on b.ida = c.ida and b.idb = c.idb
where b.ida != 40
order by b.ida, b.idb
