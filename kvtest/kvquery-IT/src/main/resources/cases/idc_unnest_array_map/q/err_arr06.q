select id
from User as $u, unnest($u.addresses[] as $address, $address.phones[][$element.kind='work'] as $phone)
