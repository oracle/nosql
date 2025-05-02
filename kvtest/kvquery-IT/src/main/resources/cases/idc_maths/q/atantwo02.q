#atan2 of null and NaN and missing field

select
  trunc(t.iv,7) as iv, trunc(atan2(t.iv,t.fv),7),
  trunc(t.lv,7) as lv, trunc(atan2(t.lv,null),7),
  trunc(t.fv,7) as fv, trunc(atan2(t.fv,t.fv),7),
  trunc(t.dv,7) as dv, trunc(atan2(t.dv,null),7),
  trunc(t.nv,7) as nv, trunc(atan2(t.nv,0),7),
  trunc(t.doc.iv,7), trunc(atan2(t.doc.iv,-1.5),7),
  trunc(t.doc.lv,7), trunc(atan2(t.doc.lv,1),7),
  trunc(t.doc.fv,7), trunc(atan2(t.doc.fv,5),7),
  trunc(t.doc.dv,7), trunc(atan2(t.doc.dv,1/3.0),7),
  trunc(t.doc.nv,7), trunc(atan2(t.doc.nv,null),7),
  trunc(t.doc.fv,7), trunc(atan2(t.doc.fv,-5.7976931348623157e+308),7),
  trunc(t.doc.fv,7), trunc(atan2(t.doc.fv,3.7976931348623157e+308),7)
from functional_test t where id=4

