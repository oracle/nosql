# partial key
declare
$ext1_1 integer; // 3
select id
from Foo t
where t.rec.a = 10 and t.rec.c.ca =any $ext1_1
