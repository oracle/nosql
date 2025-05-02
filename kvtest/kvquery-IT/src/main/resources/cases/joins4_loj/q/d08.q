# ON clause + arithmetic expressions + json index A.B(b4.comment as string)
select b.ida1, b.idb1, b.idb2, b.b4, d.idd1, d2, d3
from A.B b left outer join A.B.D d on b.ida1 = d.ida1 and b.idb1 = d.idb1 and
                                      b.idb2 = d.idb2 and d2 + b.idb2 > 100
where b.b4.comment='****'
