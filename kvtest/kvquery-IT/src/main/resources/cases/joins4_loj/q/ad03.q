# aggregate function + group by
select b.ida1, min(a2) as min_of_a2, max(a2) as max_of_a2,
       sum(c.idc2) as sum_of_idc2
from A.B b left outer join A a on b.ida1 = a.ida1
           left outer join A.B.C c on b.ida1 = c.ida1 and b.idb1 = c.idb1 and
                                      b.idb2 = c.idb2
group by b.ida1
