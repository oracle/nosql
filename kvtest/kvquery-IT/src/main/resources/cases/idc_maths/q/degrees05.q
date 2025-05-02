#degrees of min values

select
trunc(t.iv,7) as iv, trunc(degrees(t.iv),7),
trunc(t.lv,7) as lv, trunc(degrees(t.lv),7),
trunc(t.fv,7) as fv, trunc(degrees(t.fv),7),
trunc(t.dv,7) as dv, trunc(degrees(t.dv),7),
trunc(t.nv,7) as nv, trunc(degrees(t.nv),7),
trunc(t.doc.iv,7) as jsoniv, trunc(degrees(t.doc.iv),7) as degreesjsoniv,
trunc(t.doc.lv,7) as jsonlv, trunc(degrees(t.doc.lv),7) as degreesjsonlv,
trunc(t.doc.fv,7) as jsonfv, trunc(degrees(t.doc.fv),7) as degreesjsonfv,
trunc(t.doc.dv,7) as jsondv, trunc(degrees(t.doc.dv),7) as degreesjsondv,
trunc(t.doc.nv,7) as jsonnv, trunc(degrees(t.doc.nv),7) as degreesjsonnv
from functional_test t where id=8