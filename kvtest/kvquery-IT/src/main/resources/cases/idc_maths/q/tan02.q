#tan of null,NaN and missing field

select
trunc(t.iv,7) as iv, trunc(sqrt(t.iv),7),
trunc(t.lv,7) as lv, trunc(sqrt(t.lv),7),
trunc(t.fv,7) as fv, trunc(sqrt(t.fv),7),
trunc(t.dv,7) as dv, trunc(sqrt(t.dv),7),
trunc(t.nv,7) as nv, trunc(sqrt(t.nv),7),
trunc(t.doc.iv,7) as jsoniv, trunc(sqrt(t.doc.iv),7) as sqrtjsoniv,
trunc(t.doc.lv,7) as jsonlv, trunc(sqrt(t.doc.lv),7) as sqrtjsonlv,
trunc(t.doc.fv,7) as jsonfv, trunc(sqrt(t.doc.fv),7) as sqrtjsonfv,
trunc(t.doc.dv,7) as jsondv, trunc(sqrt(t.doc.dv),7) as sqrtjsondv,
trunc(t.doc.nv,7) as jsonnv, trunc(sqrt(t.doc.nv),7) as sqrtjsonnv,
trunc(t.doc.zv,7) as jsonzv, trunc(sqrt(t.doc.zv),7) as sqrtjsonzv
from functional_test t where id=4
