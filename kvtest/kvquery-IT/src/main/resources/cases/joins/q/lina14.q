select *
from nested tables(A.B.C c ancestors(A a, A.B b))
order by c.ida
