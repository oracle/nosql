#abs of nulls
select t.ic, abs(t.ic) as absic,
       t.lc, abs(t.lc) as abslc,
       t.fc, abs(t.fc) as absfc,
       t.dc, abs(t.dc) as absdc,
       t.nc, abs(t.nc) as absnc
 from math_test t where id=5

