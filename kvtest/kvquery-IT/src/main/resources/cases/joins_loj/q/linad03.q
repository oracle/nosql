declare $ext1 integer; // = 30
select c.ida as c_ida, c.idb as c_idb, c.idc as c_idc,
       a.ida as a_ida, a.a2,
       b.ida as b_ida, b.idb as b_idb,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd
from A.B.C c left outer join A a on c.ida = a.ida
             left outer join A.B b on c.ida = b.ida and c.idb = b.idb
             left outer join A.B.C.D d on c.ida = d.ida and c.idb = d.idb and 
                                          c.idc = d.idc and 
                                          (b.b2 = 45 or b.b2 is null and 
                                           d.c3 < $ext1)
order by c.ida, c.idb
