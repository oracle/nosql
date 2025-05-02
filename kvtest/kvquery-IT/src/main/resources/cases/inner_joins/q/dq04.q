SELECT msgs.msgid, msgs.content.date, p.userName
FROM  nested tables(profile p descendants(profile.sent sent, profile.deleted deleted)),
      profile.messages msgs
WHERE msgs.uid = p.uid and
      (msgs.msgid = sent.msgid or msgs.msgid = deleted.msgid)
