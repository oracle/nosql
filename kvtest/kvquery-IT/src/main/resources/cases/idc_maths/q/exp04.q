#exp of max values

select
trunc(t.iv,7) as iv, trunc(exp(t.iv),7),
trunc(t.lv,7) as lv, trunc(exp(t.lv),7),
trunc(t.fv,7) as fv, trunc(exp(t.fv),7),
trunc(t.dv,7) as dv, trunc(exp(t.dv),7),
trunc(t.nv,7) as nv, trunc(exp(t.nv),7),
trunc(t.doc.iv,7) as jsoniv, trunc(exp(t.doc.iv),7) as expjsoniv,
trunc(t.doc.lv,7) as jsonlv, trunc(exp(t.doc.lv),7) as expjsonlv,
trunc(t.doc.fv,7) as jsonfv, trunc(exp(t.doc.fv),7) as expjsonfv,
trunc(t.doc.dv,7) as jsondv, trunc(exp(t.doc.dv),7) as expjsondv,
trunc(t.doc.nv,7) as jsonnv, trunc(exp(t.doc.nv),7) as expjsonnv
from functional_test t where id=6