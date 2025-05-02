select id
from User_json as $u, unnest(info.addresses[] as $address)
where $address.state = 'CA'
