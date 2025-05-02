SELECT msgs.msgid as messages_mid,
       inbox.msgid as inbox_mid,
       msgs.content.date as date
FROM profile.messages msgs, profile.inbox inbox
WHERE msgs.uid = inbox.uid and msgs.content.date = inbox.content.date and
      msgs.content.sender = "markos"
