#
# complete key and always true pred
#
declare
$ext4_1 integer; // 20
select id
from Foo t
where t.rec.a = 10 and
      t.rec.c.ca >=any 3 and t.rec.c.ca =any $ext4_1 and
      t.rec.f = 4.5
