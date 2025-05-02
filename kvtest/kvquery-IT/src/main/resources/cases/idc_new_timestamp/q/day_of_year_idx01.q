select id, day_of_year(t.str4)
from jsonCollection_test t
where day_of_year(t.str4) >= 150