#
# Return the number of messages exchanged by each user
# with a salary greater than 100
#
select count($m)
from nested tables (
     users u
     descendants(users.folders.messages $m ON
                 time >= cast("2017-12-12T00:00:00.0" as timestamp(3))))
where salary > 100
