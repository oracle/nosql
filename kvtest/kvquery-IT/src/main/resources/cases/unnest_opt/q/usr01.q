select id
from User as $u, unnest($u.addresses.phones[][] as $phone)
