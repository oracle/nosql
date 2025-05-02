# aggregate function + JSON path expression + map field step expression
select b.ida1, min(b.b4.comment) as min_of_b4,
       max(b.b4.comment) as max_of_b4, sum(c.c3.ckey1) as sum_of_c3
from A.B b left outer join A a on b.ida1 = a.ida1
           left outer join A.B.C c on b.ida1 = c.ida1 and b.idb1 = c.idb1 and
                                      b.idb2 = c.idb2
group by b.ida1
