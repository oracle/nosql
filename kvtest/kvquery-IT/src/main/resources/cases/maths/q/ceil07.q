#ceil of NaN
select
 t.fc, ceil(t.fc),
 t.dc, ceil(t.dc),
 t.doc.fc, ceil(t.doc.fc),
 t.doc.dc, ceil(t.doc.dc) 
from math_test t where t.id=9

