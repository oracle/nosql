#floor of NaN
select
 t.fc, floor(t.fc),
 t.dc, floor(t.dc),
 t.doc.fc, floor(t.doc.fc),
 t.doc.dc, floor(t.doc.dc) 
from math_test t where t.id=9

