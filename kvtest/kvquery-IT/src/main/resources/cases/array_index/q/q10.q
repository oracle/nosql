#
# partial key and range; only one multi-key pred pushed, the other
# is always true
#  
select id
from Foo t 
where t.rec.a = 10 and t.rec.c[].ca <=any 3 and t.rec.c[].ca <any 3
