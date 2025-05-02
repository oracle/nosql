#ceil types
select
  ceil(t.ic) is of type(integer),
  ceil(t.lc) is of type(long),
  ceil(t.fc) is of type(float),
  ceil(t.dc) is of type(double), 
  ceil(t.nc) is of type(number),
  ceil(t.doc.ic) is of type(integer),
  ceil(t.doc.lc) is of type(long),
  ceil(t.doc.fc) is of type(double),
  ceil(t.doc.dc) is of type(double),
  ceil(t.doc.nc) is of type(double)
from math_test t where t.id=1

