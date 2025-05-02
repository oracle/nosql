SELECT msgs.msgid, msgs.content.sender
FROM profile.inbox inbox, profile.messages msgs
WHERE msgs.uid = inbox.uid and msgs.content.date = inbox.content.date and
      msgs.content.sender = "markos"
