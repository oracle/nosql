#
# range only
#
select t.info.address.state, t.info.address.city 
from foo t
where 'MA' <= t.info.address.state
order by t.info.address.state, t.info.address.city
