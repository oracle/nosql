#round of null and NaN and missing field

select
  t.iv, round(t.iv,t.fv),
  t.lv, round(t.lv,null),
  t.fv, round(t.fv,t.fv),
  t.dv, round(t.dv,null),
  t.nv, round(t.nv,0),
  t.doc.iv, round(t.doc.iv,-1.5),
  t.doc.lv, round(t.doc.lv,1),
  t.doc.fv, round(t.doc.fv,5),
  t.doc.dv, round(t.doc.dv,1/3.0),
  t.doc.nv, round(t.doc.nv,null),
  t.doc.fv, round(t.doc.fv,-5.7976931348623157e+308),
  t.doc.fv, round(t.doc.fv,3.7976931348623157e+308)
from functional_test t where id=4

