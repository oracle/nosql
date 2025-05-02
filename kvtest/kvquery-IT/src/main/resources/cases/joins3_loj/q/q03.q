#
# Return the number of messages exchanged by each user
# with a salary greater than 100
#
select count($m)
from users u left outer join users.folders.messages $m ON
                 u.uid = $m.uid and
                 time >= cast("2017-12-12T00:00:00.0" as timestamp(3))
where salary > 100
