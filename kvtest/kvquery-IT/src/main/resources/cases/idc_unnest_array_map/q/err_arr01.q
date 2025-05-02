select id, $phone.number
from User as $u, unnest($u.firstName as $first)
