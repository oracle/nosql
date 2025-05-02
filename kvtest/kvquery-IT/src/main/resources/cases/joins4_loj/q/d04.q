# size() + cast()
select a.ida1, g.idg1, g.g3
from A a left outer join A.G g on a.ida1 = g.ida1 and size(g.g3) > 1
where g.idg1 > cast('2018-02-01' as timestamp)
