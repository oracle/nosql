select id
from User as $u, unnest(addresses.phones as $phone)
