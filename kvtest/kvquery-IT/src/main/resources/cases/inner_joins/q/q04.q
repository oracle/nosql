SELECT msgs.msgid, msgs.content.sender
FROM profile.inbox inbox, profile.messages msgs
WHERE msgs.uid = inbox.uid and msgs.msgid = inbox.msgid and
      msgs.content.sender = "markos"
