#log tests for NaN and null
select
  trunc(log(t.iv,10),4) as log_null_10,
  trunc(log(10, 10),4) as log_10_10,
  trunc(log(t.lv,null),4) as log_null_null,
  trunc(log(t.fv,null),4) as log_NaN_null,
  trunc(log(t.dv,2),4) as log_NaN_2,
  trunc(log(t.doc.iv,2),4) as log_null_2,
  trunc(log(t.doc.fv,1.7976931348623157e+308),4) as log_NaN_inf,
  trunc(log(t.doc.dv,-1.7976931348623157e+308),4) as log_NaN_neginf,
  trunc(log(t.numArr,1.7976931348623157e+308),4) as log_null_inf,
  trunc(log(t.doc.numArr,-1.7976931348623157e+308),4) as log_null_neginf
from functional_test t where id =4
