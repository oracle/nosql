select *
from nested tables(A a 
                   descendants(A.B b ON b.b1 is null or b.c1 = a.c1,
                               A.B.C,
                               A.B.C.D d))
order by a.ida
