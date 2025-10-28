select id, row_metadata($t).age
from foo $t
where row_metadata($t).age > 10
order by row_metadata($t).address.state,
         row_metadata($t).address.city,
         row_metadata($t).age
