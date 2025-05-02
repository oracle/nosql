select b.ida as b_ida, b.idb as b_idb,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd
from A.B b left outer join A.B.C c on b.ida = c.ida and b.idb = c.idb
           left outer join A.B.C.D d on c.ida = d.ida and c.idb = d.idb and 
                                        c.idc = d.idc
where b.ida != 40
order by b.ida, b.idb
