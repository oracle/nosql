select d.ida as d_ida, d.idb as d_idb, count(*) as cnt
from nested tables(A.B.C.D d ancestors(A a, A.B b, A.B.C c on c.c1 >= 15))
where d.ida = 40 and d.idb > 0 and d.idd > 5
group by d.ida, d.idb
