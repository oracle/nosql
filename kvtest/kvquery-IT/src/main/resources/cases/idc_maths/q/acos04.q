#acos of max values

select
  trunc(t.iv,7) as iv, trunc(acos(t.iv),7),
  trunc(t.lv,7) as lv, trunc(acos(t.lv),7),
  trunc(t.fv,7) as fv, trunc(acos(t.fv),7),
  trunc(t.dv,7) as dv, trunc(acos(t.dv),7),
  trunc(t.nv,7) as nv, trunc(acos(t.dv),7),
  trunc(t.nv,7), trunc(acos(t.nv),7),
    trunc(t.doc.iv,7) as jsoniv, trunc(acos(t.doc.iv),7) as acosjsoniv,
    trunc(t.doc.lv,7) as jsonlv, trunc(acos(t.doc.lv),7) as acosjsonlv,
    trunc(t.doc.fv,7) as jsonfv, trunc(acos(t.doc.fv),7) as acosjsonfv,
    trunc(t.doc.dv,7) as jsondv, trunc(acos(t.doc.dv),7) as acosjsondv,
    trunc(t.doc.nv,7) as jsonnv, trunc(acos(t.doc.nv),7) as acosjsonnv
from functional_test t where id=6