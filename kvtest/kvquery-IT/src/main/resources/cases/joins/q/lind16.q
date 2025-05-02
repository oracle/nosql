select *
from nested tables(A a descendants(A.B b, A.B.C.D d ON d1 < 20, A.B.C c ON c2 > 20))
order by a.ida
