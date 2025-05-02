select /* FORCE_INDEX(users idx_year_month) */ id
from users u
where substring(u.address.startDate, 0, 4) = "2021" 
