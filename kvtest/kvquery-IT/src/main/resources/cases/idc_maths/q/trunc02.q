#trunc of null and NaN and missing field

select
  t.iv, trunc(t.iv,t.fv),
  t.lv, trunc(t.lv,null),
  t.fv, trunc(t.fv,t.fv),
  t.dv, trunc(t.dv,null),
  t.nv, trunc(t.nv,0),
  t.doc.iv, trunc(t.doc.iv,-1.5),
  t.doc.lv, trunc(t.doc.lv,1),
  t.doc.fv, trunc(t.doc.fv,5),
  t.doc.dv, trunc(t.doc.dv,1/3.0),
  t.doc.nv, trunc(t.doc.nv,null),
  t.doc.fv, trunc(t.doc.fv,-5.7976931348623157e+308),
  t.doc.fv, trunc(t.doc.fv,3.7976931348623157e+308)
from functional_test t where id=4

