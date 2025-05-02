select *
from A.B.C c left outer join A a on c.ida = a.ida
             left outer join A.B b on c.ida = b.ida and c.idb = b.idb
order by c.ida
