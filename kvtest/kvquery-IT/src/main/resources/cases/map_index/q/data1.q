#
# partial key
#
select id
from Foo t
where t.g = 5 and t.rec.c.values().ca =any 10
