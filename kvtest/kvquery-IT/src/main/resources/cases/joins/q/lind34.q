select b.ida, count(b.idb) as count
from nested tables(A.B b ancestors(A a))
where a.ida is not null
group by b.ida
order by b.ida desc
