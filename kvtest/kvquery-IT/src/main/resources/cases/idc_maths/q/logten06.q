#log10 of random positive values

select
trunc(t.iv,7) as iv, trunc(log10(t.iv),7),
trunc(t.lv,7) as lv, trunc(log10(t.lv),7),
trunc(t.fv,7) as fv, trunc(log10(t.fv),7),
trunc(t.dv,7) as dv, trunc(log10(t.dv),7),
trunc(t.nv,7) as nv, trunc(log10(t.nv),7),
trunc(t.doc.iv,7) as jsoniv, trunc(log10(t.doc.iv),7) as log10jsoniv,
trunc(t.doc.lv,7) as jsonlv, trunc(log10(t.doc.lv),7) as log10jsonlv,
trunc(t.doc.fv,7) as jsonfv, trunc(log10(t.doc.fv),7) as log10jsonfv,
trunc(t.doc.dv,7) as jsondv, trunc(log10(t.doc.dv),7) as log10jsondv,
trunc(t.doc.nv,7) as jsonnv, trunc(log10(t.doc.nv),7) as log10jsonnv
from functional_test t where id=1