#tan of min and boundary values

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
trunc(t.doc.pi1,7) as jsonpi1, trunc(tan(t.doc.pi1),7) as tanjsonpi1,
trunc(t.doc.pi2,7) as jsonpi2, trunc(tan(t.doc.pi2),7) as tanjsonpi2,
trunc(t.doc.pi3,7) as jsonpi3, trunc(tan(t.doc.pi3),7) as tanjsonpi3,
trunc(t.doc.pi4,7) as jsonpi4, trunc(tan(t.doc.pi4),7) as tanjsonpi4
from functional_test t where id=8