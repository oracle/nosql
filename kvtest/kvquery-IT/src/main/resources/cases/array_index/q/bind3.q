#
# partial key and range; only one multi-key pred pushed
#
declare
$ext3_1 integer; // 10
select id
from Foo t
where t.rec.a = 10 and $ext3_1 <=any t.rec.c.ca and t.rec.c.ca <any $ext3_1
