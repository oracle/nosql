# array filter + array slice step expression + sequence comparison
select a.ida1, d.idd1, d.d4[0:2] as d4
from A a left outer join A.B.D d on a.ida1 = d.ida1
where d.d4[] =any 'abc'
