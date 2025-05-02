select id
from User as $u, unnest($u.addresses[] as $address),  unnest($u.sddresses.phones[][] as $phone)
