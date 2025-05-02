#
# TODO: make this query work in the cloud
#
select case
       when 5.2 < b.info.address.state and
            b.info.address.state < 5.4 then 5.3
       else b.info.address.state
       end as state, 
       count(*) as count
from bar b
group by b.info.address.state
