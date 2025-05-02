select id
from User as $u, unnest($u.addresses[] as $address, $u.addresses.phones[] as $phone)
