#round with index
select id,trunc(fc,2) as fc,round(trunc(fc,2)) from math_test where round(fc)>0 order by id
