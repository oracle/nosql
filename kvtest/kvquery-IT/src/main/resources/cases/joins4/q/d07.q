# count(*) + group by + not operator + is null
select a.ida1, count(*) as count
from nested tables (A a descendants (A.B b on not b3 is null))
group by a.ida1
