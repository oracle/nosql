#floor types
select
  floor(t.ic) is of type(integer),
  floor(t.lc) is of type(long),
  floor(t.fc) is of type(float),
  floor(t.dc) is of type(double), 
  floor(t.nc) is of type(number),
  floor(t.doc.ic) is of type(integer),
  floor(t.doc.lc) is of type(long),
  floor(t.doc.fc) is of type(double),
  floor(t.doc.dc) is of type(double),
  floor(t.doc.nc) is of type(double)
from math_test t where t.id=1

