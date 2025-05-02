select *
from nested tables(A a descendants(A.B b, A.B.C.D d, A.B.C c ON c.c2 + d.d1 < 10))
order by a.ida
