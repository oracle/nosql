select id
from User as $u, unnest($u.addresses[] as $address), $u.address.phones[] as $phone
