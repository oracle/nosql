SELECT msgs.msgid, msgs.content
FROM profile.messages msgs, profile.inbox inbox
WHERE msgs.msgid = inbox.msgid
