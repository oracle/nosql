# order by + limit + offset
select a.ida1, c.idc1, e.ide1, g.idg1
from nested tables (A a descendants (A.B.C c, A.B.C.E e on ide1 = 'tok3' or ide1='tok4', A.G.H g))
order by a.ida1
limit 5
offset 11
