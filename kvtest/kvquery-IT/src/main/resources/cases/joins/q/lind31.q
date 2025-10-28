select a.ida, b.ida, b.idb
from nested tables(A a descendants(A.B b))
where a.ida = 40
order by b.idb
