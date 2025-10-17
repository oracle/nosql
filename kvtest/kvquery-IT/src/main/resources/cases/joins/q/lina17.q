select b.ida, b.idb
from nested tables(A.B b ancestors(A a))
where b.ida = 10 and
      b.b1 = a.a1
order by b.ida, b.idb
