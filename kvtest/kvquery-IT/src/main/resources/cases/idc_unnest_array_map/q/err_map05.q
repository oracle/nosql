select id
from User as $u, unnest($u.addresses[] as $address, $address.phones[][] as $phone, Su.children.values() as $child)
