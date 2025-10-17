select a.ida, b.ida, b.idb
from A a, A.B b
where a.ida = b.ida
order by a.ida desc, b.idb desc
