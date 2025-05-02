#abs of 0's and infinity numbers
select t.iv, abs(t.iv) as absiv,
       t.lv, abs(t.lv) as abslv,
       trunc(t.fv,6) as fv, trunc(abs(t.fv),6) as absfv,
       trunc(t.dv,12) as dv, trunc(abs(t.dv),12) as absdv,
       trunc(t.nv,12) as nv, trunc(abs(t.nv),12) as absnv
 from functional_test t where id=3