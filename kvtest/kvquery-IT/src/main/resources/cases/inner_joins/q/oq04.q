SELECT msgs2.content.size, msgs2.msgid as msg2, msgs1.msgid as msg1, p.userName
FROM profile p, profile.messages msgs2, profile.messages msgs1
WHERE msgs1.uid = p.uid and
      msgs2.uid = p.uid and
      msgs1.content.receivers[] =any msgs2.content.sender and
      msgs2.content.size >= 30
order by msgs2.content.size, msgs2.uid, msgs2.msgid
