select a.ida as a_ida,
       b.ida as b_ida, b.idb as b_idb
from A a left outer join A.B b on a.ida = b.ida
where a.ida != 40
order by a.ida
