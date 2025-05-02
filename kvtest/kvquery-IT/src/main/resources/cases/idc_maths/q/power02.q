#power of NaN, null and missing values

select
  trunc(t.iv,7) as iv, trunc(power(t.iv,1),7) as pow1,
  trunc(t.lv,7) as lv, trunc(power(t.lv,-2),7) as powneg2,
  trunc(t.fv,7) as fv, trunc(power(t.fv,1/2.0),7) as pow1by2,
  trunc(t.dv,7) as dv, trunc(power(t.dv,-1/2.0),7) as pow1neg1by2,
  trunc(t.nv,7) as nv, trunc(power(t.nv,null),7) as pownull,
  trunc(t.doc.iv,7) as jsoniv, trunc(power(t.doc.iv,0),7) as powerjsoniv0,
  trunc(t.doc.lv,7) as jsonlv, trunc(power(t.doc.lv,-0),7) as powerjsonlvneg0,
  trunc(t.doc.fv,7) as jsonfv, trunc(power(t.doc.fv,1/2.0),7) as powerjsonfv1by2,
  trunc(t.doc.dv,7) as jsondv, trunc(power(t.doc.dv,1.7976931348623157e+308),7) as powerjsondvinfinity,
  trunc(t.doc.nv,7) as jsonnv, trunc(power(t.doc.nv,-1.7976931348623157e+308),7) as powerjsonnvneginfinity,
  trunc(t.doc.zv,7) as jsonzv, trunc(power(t.doc.zv,null),7) as powerjsonzvNaN
from functional_test t where id=4
