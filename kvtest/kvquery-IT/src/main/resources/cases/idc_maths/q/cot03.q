#cot of infinities and zeroes

select
trunc(t.iv,7) as iv, trunc(cot(t.iv),7),
trunc(t.lv,7) as lv, trunc(cot(t.lv),7),
trunc(t.fv,7) as fv, trunc(cot(t.fv),7),
trunc(t.dv,7) as dv, trunc(cot(t.dv),7),
trunc(t.nv,7) as nv, trunc(cot(t.nv),7),
trunc(t.doc.iv,7) as jsoniv, trunc(cot(t.doc.iv),7) as cotjsoniv,
trunc(t.doc.lv,7) as jsonlv, trunc(cot(t.doc.lv),7) as cotjsonlv,
trunc(t.doc.fv,7) as jsonfv, trunc(cot(t.doc.fv),7) as cotjsonfv,
trunc(t.doc.dv,7) as jsondv, trunc(cot(t.doc.dv),7) as cotjsondv,
trunc(t.doc.nv,7) as jsonnv, trunc(cot(t.doc.nv),7) as cotjsonnv
from functional_test t where id=3