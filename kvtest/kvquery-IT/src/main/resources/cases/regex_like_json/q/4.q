# regex like operator source parameter is json array 
#
select id, regex_like(f.info.address.phones[0].areacode, "Sal.*")
from Foo f
where id = 4


