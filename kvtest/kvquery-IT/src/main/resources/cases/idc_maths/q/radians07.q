#radians of random negative values

select
trunc(t.iv,7) as iv, trunc(radians(t.iv),7),
trunc(t.lv,7) as lv, trunc(radians(t.lv),7),
trunc(t.fv,7) as fv, trunc(radians(t.fv),7),
trunc(t.dv,7) as dv, trunc(radians(t.dv),7),
trunc(t.nv,7) as nv, trunc(radians(t.nv),7),
trunc(t.doc.iv,7) as jsoniv, trunc(radians(t.doc.iv),7) as radiansjsoniv,
trunc(t.doc.lv,7) as jsonlv, trunc(radians(t.doc.lv),7) as radiansjsonlv,
trunc(t.doc.fv,7) as jsonfv, trunc(radians(t.doc.fv),7) as radiansjsonfv,
trunc(t.doc.dv,7) as jsondv, trunc(radians(t.doc.dv),7) as radiansjsondv,
trunc(t.doc.nv,7) as jsonnv, trunc(radians(t.doc.nv),7) as radiansjsonnv
from functional_test t where id=2