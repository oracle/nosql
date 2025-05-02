select a.ida as a_ida,
       b.ida as b_ida, b.idb as b_idb
from nested tables(A a descendants(A.B b))
where a.ida = 40
order by b.idb desc
