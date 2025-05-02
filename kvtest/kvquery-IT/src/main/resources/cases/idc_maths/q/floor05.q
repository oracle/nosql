#floor of boundary values

select
trunc(t.iv,7) as iv, trunc(floor(t.iv),7),
trunc(t.lv,7) as lv, trunc(floor(t.lv),7),
trunc(t.fv,7) as fv, trunc(floor(t.fv),7),
trunc(t.dv,7) as dv, trunc(floor(t.dv),7),
trunc(t.nv,7) as nv, trunc(floor(t.nv),7),
trunc(t.doc.iv,7) as jsoniv, trunc(floor(t.doc.iv),7) as floorjsoniv,
trunc(t.doc.lv,7) as jsonlv, trunc(floor(t.doc.lv),7) as floorjsonlv,
trunc(t.doc.fv,7) as jsonfv, trunc(floor(t.doc.fv),7) as floorjsonfv,
trunc(t.doc.dv,7) as jsondv, trunc(floor(t.doc.dv),7) as floorjsondv,
trunc(t.doc.nv,7) as jsonnv, trunc(floor(t.doc.nv),7) as floorjsonnv,
trunc(t.doc.p1,7) as jsonp1, trunc(floor(t.doc.p1),7) as floorjsonp1,
trunc(t.doc.p2,7) as jsonp2, trunc(floor(t.doc.p2),7) as floorjsonp2,
trunc(t.doc.p3,7) as jsonp3, trunc(floor(t.doc.p3),7) as floorjsonp3,
trunc(t.doc.p4,7) as jsonp4, trunc(floor(t.doc.p4),7) as floorjsonp4
from functional_test t where id=9