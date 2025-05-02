#
# partial key and range; only one multi-key pred pushed, the other
# is always true
#  
declare
$ext5_1 integer; // 6
select id
from Foo t 
where t.rec.a = 10 and t.rec.c.ca >=any $ext5_1 and t.rec.c.ca >any 6
