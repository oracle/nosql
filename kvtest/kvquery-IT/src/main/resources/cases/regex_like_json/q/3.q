#
# regex like operator in projection. Source parameter is json non string field 
#
select id, regex_like(f.info.address, "Sal.*")
from Foo f
where id = 3


