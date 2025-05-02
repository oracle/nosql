SELECT msgs.msgid, msgs.content.sender
FROM profile.messages msgs, profile.inbox inbox
WHERE msgs.uid = inbox.uid and msgs.msgid = inbox.msgid and
      msgs.content.sender = "markos"
