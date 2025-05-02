SELECT msgs1.msgid as msg1, msgs2.msgid as msg2
FROM profile.messages msgs1, profile.messages msgs2
WHERE msgs1.uid = 1 and msgs1.msgid = 21 and
      msgs2.uid = msgs1.uid and
      msgs2.content.thread_id = msgs1.content.thread_id and
      msgs1.msgid != msgs2.msgid
