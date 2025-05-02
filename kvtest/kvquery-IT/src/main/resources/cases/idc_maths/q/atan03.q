#atan of infinities and zeroes

select
trunc(t.iv,7) as iv, trunc(atan(t.iv),7),
trunc(t.lv,7) as lv, trunc(atan(t.lv),7),
trunc(t.fv,7) as fv, trunc(atan(t.fv),7),
trunc(t.dv,7) as dv, trunc(atan(t.dv),7),
trunc(t.nv,7) as nv, trunc(atan(t.nv),7),
trunc(t.doc.iv,7) as jsoniv, trunc(atan(t.doc.iv),7) as atanjsoniv,
trunc(t.doc.lv,7) as jsonlv, trunc(atan(t.doc.lv),7) as atanjsonlv,
trunc(t.doc.fv,7) as jsonfv, trunc(atan(t.doc.fv),7) as atanjsonfv,
trunc(t.doc.dv,7) as jsondv, trunc(atan(t.doc.dv),7) as atanjsondv,
trunc(t.doc.nv,7) as jsonnv, trunc(atan(t.doc.nv),7) as atanjsonnv
from functional_test t where id=3