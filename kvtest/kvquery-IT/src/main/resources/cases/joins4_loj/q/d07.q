# count(*) + group by + not operator + is null
select a.ida1, count(*) as count
from A a left outer join A.B b on a.ida1 = b.ida1 and not b3 is null
group by a.ida1
