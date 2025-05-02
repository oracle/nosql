select a.ida as a_ida, b.ida as b_ida, b.idb as b_idb , b.c1 as b_c1 
from A as a left outer join A.B as b on a.ida = b.ida
order by a.c1, a.ida
