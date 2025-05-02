#asin of null,NaN and missing field

select
    trunc(t.iv,7) as iv, trunc(asin(t.iv),7),
    trunc(t.lv,7) as lv, trunc(asin(t.lv),7),
    trunc(t.fv,7) as fv, trunc(asin(t.fv),7),
    trunc(t.dv,7) as dv, trunc(asin(t.dv),7),
    trunc(t.nv,7) as nv, trunc(asin(t.nv),7),
    trunc(t.doc.iv,7) as jsoniv, trunc(asin(t.doc.iv),7) as asinjsoniv,
    trunc(t.doc.lv,7) as jsonlv, trunc(asin(t.doc.lv),7) as asinjsonlv,
    trunc(t.doc.fv,7) as jsonfv, trunc(asin(t.doc.fv),7) as asinjsonfv,
    trunc(t.doc.dv,7) as jsondv, trunc(asin(t.doc.dv),7) as asinjsondv,
    trunc(t.doc.nv,7) as jsonnv, trunc(asin(t.doc.nv),7) as asinjsonnv,
    trunc(t.doc.zv,7) as jsonzv, trunc(asin(t.doc.zv),7) as asinjsonzv
from functional_test t where id=4
