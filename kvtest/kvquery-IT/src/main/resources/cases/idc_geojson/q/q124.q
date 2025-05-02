#Returns Returns false if the operand returns zero or more than 1 items.

select geo_is_geometry(t.id) 
from testsqlnull t
where id = 1