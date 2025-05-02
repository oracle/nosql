#abs of min numbers in json
select t.doc.iv, abs(t.doc.iv) as absiv,
       t.doc.lv, abs(t.doc.lv) as abslv,
       trunc(t.doc.fv,6) as fv, trunc(abs(t.doc.fv),6) as absfv,
       trunc(t.doc.dv,12) as dv, trunc(abs(t.doc.dv),12) as absdv,
       trunc(t.doc.nv,12) as nv, trunc(abs(t.doc.nv),12) as absnv
 from functional_test t where id=5