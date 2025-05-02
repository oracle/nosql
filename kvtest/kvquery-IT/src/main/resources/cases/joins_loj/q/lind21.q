select /*+ FORCE_INDEX(A a_idx_c1_a2) */
       a.ida as a_ida, a.c1,
       b.ida as b_ida, b.idb as b_idb,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd 
from A a left outer join A.B b on a.ida = b.ida
         left outer join A.B.C c on b.ida = c.ida and b.idb = c.idb
         left outer join A.B.C.D d on c.ida = d.ida and c.idb = d.idb and 
                                      c.idc = d.idc
order by a.c1
