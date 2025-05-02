SELECT msgs.msgid, msgs.content.date, p.userName
FROM  profile p, profile.messages msgs, profile.inbox inbox, profile.deleted deleted
WHERE p.uid = msgs.uid and
      inbox.uid = deleted.uid and
      p.uid = inbox.uid and
      msgs.msgid = inbox.msgid and
      msgs.content.sender = "markos"
