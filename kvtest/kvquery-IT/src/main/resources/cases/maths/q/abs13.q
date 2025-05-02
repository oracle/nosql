#abs of NaN
select t.fc, abs(t.fc) as absfc,
       t.dc, abs(t.dc) as absdc
 from math_test t where id=9

