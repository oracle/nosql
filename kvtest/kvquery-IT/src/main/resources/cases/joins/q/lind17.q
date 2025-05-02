select *
from nested tables(A a descendants(A.B b, A.B.C c, A.B.C.D d ON c.c2 < 10))
order by a.ida
