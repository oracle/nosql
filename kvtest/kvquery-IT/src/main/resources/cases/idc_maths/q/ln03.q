#ln of infinities and zeroes

select
trunc(t.iv,7) as iv, trunc(ln(t.iv),7),
trunc(t.lv,7) as lv, trunc(ln(t.lv),7),
trunc(t.fv,7) as fv, trunc(ln(t.fv),7),
trunc(t.dv,7) as dv, trunc(ln(t.dv),7),
trunc(t.nv,7) as nv, trunc(ln(t.nv),7),
trunc(t.doc.iv,7) as jsoniv, trunc(ln(t.doc.iv),7) as lnjsoniv,
trunc(t.doc.lv,7) as jsonlv, trunc(ln(t.doc.lv),7) as lnjsonlv,
trunc(t.doc.fv,7) as jsonfv, trunc(ln(t.doc.fv),7) as lnjsonfv,
trunc(t.doc.dv,7) as jsondv, trunc(ln(t.doc.dv),7) as lnjsondv,
trunc(t.doc.nv,7) as jsonnv, trunc(ln(t.doc.nv),7) as lnjsonnv
from functional_test t where id=3