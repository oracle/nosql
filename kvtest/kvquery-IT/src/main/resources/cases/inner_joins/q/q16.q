SELECT msgs.msgid, msgs.content
FROM profile.messages msgs, profile.inbox inbox, profile.sent sent
WHERE msgs.uid = inbox.uid and msgs.msgid = inbox.msgid
