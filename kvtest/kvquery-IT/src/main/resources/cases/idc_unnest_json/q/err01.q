select id, $phone.number
from User_json as $u, unnest($u.info.addresses[] as $address, $address.phones as $phone)
