#ceil of boundary values

select
trunc(t.iv,7) as iv, trunc(ceil(t.iv),7),
trunc(t.lv,7) as lv, trunc(ceil(t.lv),7),
trunc(t.fv,7) as fv, trunc(ceil(t.fv),7),
trunc(t.dv,7) as dv, trunc(ceil(t.dv),7),
trunc(t.nv,7) as nv, trunc(ceil(t.nv),7),
trunc(t.doc.iv,7) as jsoniv, trunc(ceil(t.doc.iv),7) as ceiljsoniv,
trunc(t.doc.lv,7) as jsonlv, trunc(ceil(t.doc.lv),7) as ceiljsonlv,
trunc(t.doc.fv,7) as jsonfv, trunc(ceil(t.doc.fv),7) as ceiljsonfv,
trunc(t.doc.dv,7) as jsondv, trunc(ceil(t.doc.dv),7) as ceiljsondv,
trunc(t.doc.nv,7) as jsonnv, trunc(ceil(t.doc.nv),7) as ceiljsonnv,
trunc(t.doc.p1,7) as jsonp1, trunc(ceil(t.doc.p1),7) as ceiljsonp1,
trunc(t.doc.p2,7) as jsonp2, trunc(ceil(t.doc.p2),7) as ceiljsonp2,
trunc(t.doc.p3,7) as jsonp3, trunc(ceil(t.doc.p3),7) as ceiljsonp3,
trunc(t.doc.p4,7) as jsonp4, trunc(ceil(t.doc.p4),7) as ceiljsonp4
from functional_test t where id=9