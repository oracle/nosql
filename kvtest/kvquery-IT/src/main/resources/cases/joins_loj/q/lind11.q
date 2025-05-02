select a.ida as a_ida,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd
from A a left outer join A.B.C.D d on a.ida = d.ida
where a.ida != 40
order by a.ida
