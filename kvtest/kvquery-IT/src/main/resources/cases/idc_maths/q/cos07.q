#cos of random negative values

select
trunc(t.iv,7) as iv, trunc(cos(t.iv),7),
trunc(t.lv,7) as lv, trunc(cos(t.lv),7),
trunc(t.fv,7) as fv, trunc(cos(t.fv),7),
trunc(t.dv,7) as dv, trunc(cos(t.dv),7),
trunc(t.nv,7) as nv, trunc(cos(t.nv),7),
trunc(t.doc.iv,7) as jsoniv, trunc(cos(t.doc.iv),7) as cosjsoniv,
trunc(t.doc.lv,7) as jsonlv, trunc(cos(t.doc.lv),7) as cosjsonlv,
trunc(t.doc.fv,7) as jsonfv, trunc(cos(t.doc.fv),7) as cosjsonfv,
trunc(t.doc.dv,7) as jsondv, trunc(cos(t.doc.dv),7) as cosjsondv,
trunc(t.doc.nv,7) as jsonnv, trunc(cos(t.doc.nv),7) as cosjsonnv
from functional_test t where id=2