select u1.uid as u_uid,
       msg.content.sender,
       count(msg.msgid) as cnt
from nested tables (profile u1 descendants(profile.inbox inbox)),
     nested tables (profile u2 descendants(profile.messages msg))
where u1.uid = u2.uid and
      ((inbox.msgid is null and msg.msgid is null) or inbox.msgid = msg.msgid)
group by u1.uid, msg.content.sender
order by u1.uid, msg.content.sender


