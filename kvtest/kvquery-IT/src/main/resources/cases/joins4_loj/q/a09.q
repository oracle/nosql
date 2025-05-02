# secondary index idx_d_d23 on A.B.D(d2, d3)
select d.ida1, d.idd1, d2, d3, d2+d3 as sum, a2, a3
from A.B.D d left outer join A a on d.ida1 = a.ida1
where d2 > 0 and d3 > 5000

