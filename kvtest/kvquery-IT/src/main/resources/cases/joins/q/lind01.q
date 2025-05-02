select *
from nested tables(A a descendants(A.B b, A.B.C c, A.B.C.D d))
where a.ida != 40

