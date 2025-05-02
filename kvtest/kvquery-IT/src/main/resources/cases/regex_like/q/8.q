#Test Description: regex_like function in predicate. Source parameter used is a  nested field.

select id1, str 
from foo f
where regex_like(f.address.city, ".*ley")
