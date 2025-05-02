select b.ida as b_ida, b.idb as b_idb,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd
from A.B b left outer join A.B.C.D d on b.ida = d.ida and b.idb = d.idb
where b.ida != 40
order by b.ida, b.idb
