SELECT msgs2.content.size, msgs1.content.sender,
       msgs2.msgid as msg2, msgs1.msgid as msg1, p.userName
FROM profile.messages msgs2, profile.messages msgs1, profile p
WHERE msgs2.content.size >= 30 and
      msgs1.uid = p.uid and
      msgs2.uid = p.uid and
      msgs1.content.sender >= msgs2.content.sender
order by msgs2.content.size, msgs1.content.sender
