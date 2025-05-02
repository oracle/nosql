#sign of infinities and zeroes

select
  t.iv, sign(t.iv),
  t.lv, sign(t.lv),
  t.fv, sign(t.fv),
  t.dv, sign(t.dv),
  t.nv, sign(t.nv),
  t.doc.iv as jsoniv, sign(t.doc.iv) as signjsoniv,
  t.doc.lv as jsonlv, sign(t.doc.lv) as signjsonlv,
  t.doc.fv as jsonfv, sign(t.doc.fv) as signjsonfv,
  t.doc.dv as jsondv, sign(t.doc.dv) as signjsondv,
  t.doc.nv as jsonnv, sign(t.doc.nv) as signjsonnv
from functional_test t where id=3