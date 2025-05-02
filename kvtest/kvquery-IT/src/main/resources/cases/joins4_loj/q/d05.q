# arithmetic expressions + secondary index A(a3)
select a.ida1, a3, d.idd1, d2, d3, d2 + d3 as sum
from A a left outer join A.B.D d on a.ida1 = d.ida1 and d2 > 0 and d3 > 4005
where a3 = 'A33qcyUB24Iy2Vgy0YJ'
