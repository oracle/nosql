select * 
from nested tables(A.B.C.D d ancestors(A)) 
where d.d2 < 10