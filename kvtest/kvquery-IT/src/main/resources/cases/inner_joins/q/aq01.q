SELECT msgs.msgid, msgs.content.date, p.userName
FROM profile.messages msgs, nested tables(profile.inbox inbox ancestors(profile p))
WHERE msgs.uid = inbox.uid and
      msgs.msgid = inbox.msgid and
      msgs.content.sender = "markos"

