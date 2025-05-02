
select *
from users u left outer join users.folders f on u.uid = f.uid
             left outer join users.folders.messages m on f.uid = m.uid and
                                                         f.fid = m.fid
where u.uid = 10
