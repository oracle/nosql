#
# complete key and unrecognized always true pred
# TODO: recognize the always true pred
#
select id
from Foo t
where t.rec.a = 10 and
      t.rec.c.ca >=any 3 and t.rec.c.ca =any 20 and
      t.rec.f = 4.5
