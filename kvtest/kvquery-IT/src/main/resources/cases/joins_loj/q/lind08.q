select a.ida as a_ida,
       b.ida as b_ida, b.idb as b_idb,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc
from A a left outer join A.B b on a.ida = b.ida
         left outer join A.B.C c on b.ida = c.ida and b.idb = c.idb
where a.ida != 40
order by a.ida
