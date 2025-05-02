select *
from A a left outer join A.B b ON a.ida = b.ida and 
								  (b.b1 is null or b.c1 = a.c1)
          left outer join A.B.C c on b.ida = c.ida and b.idb = c.idb
          left outer join A.B.C.D d on c.ida = d.ida and c.idb = d.idb and 
          							   c.idc = d.idc
order by a.ida
