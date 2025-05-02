# order by + where + timestamp function
select g.ida1, g.idg1, g4, h.idh1, h.idh2, h3, h4
from A.G g left outer join A a on g.ida1 = a.ida1
           left outer join A.G.H h on g.ida1 = h.ida1 and g.idg1 = h.idg1
where g.g4 < cast('2018-04-01' as timestamp)
order by g.ida1, g.idg1
