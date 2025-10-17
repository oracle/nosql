select a.ida, b.ida, b.idb
from A.B b, A a
where a.ida = b.ida
order by b.ida desc, b.idb desc
