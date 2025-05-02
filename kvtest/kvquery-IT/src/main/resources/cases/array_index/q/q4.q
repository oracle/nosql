#
# partial key and range
#
select id
from Foo t
where t.rec.a = 10 and t.rec.c.ca >any 6
