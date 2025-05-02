SELECT msgs.msgid, msgs.content.date, p.userName
FROM profile p, profile.messages msgs, profile.inbox inbox
WHERE msgs.uid = inbox.uid and
      msgs.msgid = inbox.msgid and
      msgs.uid = p.uid and
      msgs.content.sender = "markos"
