select a.ida as a_ida, b.ida as b_ida, b.idb as b_idb , b.c1 as b_c1 
from nested tables (A as a descendants(A.B as b))
order by a.c1, a.ida
