#
# regex like operator source parameter is json non string field 
#
select id, f.info.address.city
from Foo f
where regex_like(f.info.address,"Sal.*")

