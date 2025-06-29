#sin of min and boundary values

select
trunc(t.iv,7) as iv, trunc(sin(t.iv),7),
trunc(t.lv,7) as lv, trunc(sin(t.lv),7),
trunc(t.fv,7) as fv, trunc(sin(t.fv),7),
trunc(t.dv,7) as dv, trunc(sin(t.dv),7),
trunc(t.nv,7) as nv, trunc(sin(t.nv),7),
trunc(t.doc.iv,7) as jsoniv, trunc(sin(t.doc.iv),7) as sinjsoniv,
trunc(t.doc.lv,7) as jsonlv, trunc(sin(t.doc.lv),7) as sinjsonlv,
trunc(t.doc.fv,7) as jsonfv, trunc(sin(t.doc.fv),7) as sinjsonfv,
trunc(t.doc.dv,7) as jsondv, trunc(sin(t.doc.dv),7) as sinjsondv,
trunc(t.doc.nv,7) as jsonnv, trunc(sin(t.doc.nv),7) as sinjsonnv,
trunc(t.doc.pi1,7) as jsonpi1, trunc(sin(t.doc.pi1),7) as sinjsonpi1,
trunc(t.doc.pi2,7) as jsonpi2, trunc(sin(t.doc.pi2),7) as sinjsonpi2,
trunc(t.doc.pi3,7) as jsonpi3, trunc(sin(t.doc.pi3),7) as sinjsonpi3
from functional_test t where id=8