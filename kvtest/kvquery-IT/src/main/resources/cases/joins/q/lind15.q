select *
from nested tables(A a descendants(A.B b, A.B.C c ON c.c2 < 10, A.B.C.D d))
order by a.ida
