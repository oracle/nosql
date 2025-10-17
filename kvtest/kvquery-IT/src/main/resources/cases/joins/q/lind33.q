select a.ida, count(b.idb) as count
from nested tables(A a descendants(A.B b))
where b.idb is not null
group by a.ida
order by a.ida desc
