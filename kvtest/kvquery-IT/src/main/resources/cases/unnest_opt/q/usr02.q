select /*+ FORCE_INDEX(User idx_state_city_age) */ id
from User as $u, unnest($u.addresses.phones[][] as $phone)
