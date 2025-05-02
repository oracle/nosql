SELECT msgs.msgid, msgs.content.date, p.userName
FROM nested tables(profile p descendants(profile.messages msgs)), profile.inbox inbox
WHERE p.uid = inbox.uid and
      msgs.msgid = inbox.msgid and
      msgs.content.sender = "markos"
