#
# partial key and range; only one multi-key pred pushed
#
select id
from Foo t
where t.rec.a = 10 and 10 <=any t.rec.c.ca and t.rec.c.ca <any 10
