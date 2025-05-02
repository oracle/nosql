#abs with types

select abs(t.ic) is of type(integer), abs(t.lc) is of type(long), abs(t.fc) is of type(double), abs(t.dc) is of type(double), abs(t.nc) is of type(number), abs(t.doc.ic) is of type(integer), abs(t.doc.lc) is of type(long), abs(t.doc.fc) is of type(double), abs(t.doc.dc) is of type(double), abs(t.doc.nc) is of type(double) from math_test t where t.id=2
