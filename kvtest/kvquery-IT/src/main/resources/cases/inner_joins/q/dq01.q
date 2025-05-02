SELECT msgs.msgid, msgs.content.date, p.userName
FROM profile.messages msgs, nested tables(profile p descendants(profile.inbox inbox))
WHERE msgs.uid = inbox.uid and
      msgs.msgid = inbox.msgid and
      msgs.content.sender = "markos"
