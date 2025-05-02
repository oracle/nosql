#abs of negative number in json
select t.doc.ic, abs(t.doc.ic) as absic,
       t.doc.lc, abs(t.doc.lc) as abslc,
       t.doc.fc, abs(t.doc.fc) as absfc,
       t.doc.dc, abs(t.doc.dc) as absdc,
       t.doc.nc, abs(t.doc.nc) as absnc
 from math_test t where id=2

