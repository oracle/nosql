select /*+ FORCE_INDEX(User idx_state_city_age) */
      $phone.areacode, count(*) as cnt
from User as $u, unnest($u.addresses.phones[][] as $phone)
group by $phone.areacode
