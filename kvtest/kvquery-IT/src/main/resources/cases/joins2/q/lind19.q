select b.ida as b_ida, b.idb as b_idb,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd
from nested tables(A.B b descendants(A.B.C.D d, A.B.C c))
where case when d.ida < 40 then b.ida + c.idc < 25 else c.idc > 5 end 
order by b.ida, b.idb
