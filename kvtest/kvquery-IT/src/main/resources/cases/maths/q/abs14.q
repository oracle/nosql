#abs of NaN in json col
select t.doc.fc, abs(t.doc.fc) as absfc,
       t.doc.dc, abs(t.doc.dc) as absdc
 from math_test t where id=9

