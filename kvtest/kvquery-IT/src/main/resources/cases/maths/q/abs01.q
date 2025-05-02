#abs of positive number
select t.ic, abs(t.ic) as absic,
       t.lc, abs(t.lc) as abslc,
       trunc(t.fc,5) as fc, trunc(abs(t.fc),5) as absfc,
       t.dc, abs(t.dc) as absdc,
       t.nc, abs(t.nc) as absnc
 from math_test t where id=1

