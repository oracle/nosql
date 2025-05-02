# order by + limit + offset
select a.ida1, c.idc1, e.ide1
from A a left outer join A.B.C c on a.ida1 = c.ida1
         left outer join A.B.C.E e on c.ida1 = e.ida1 and
                         c.idb1 = e.idb1 and c.idb2 = e.idb2 and
                         c.idc1 = e.idc1 and c.idc2 = e.idc2 and
                         (ide1 = 'tok3' or ide1='tok4')
order by a.ida1 limit 5 offset 11

