SELECT msgs1.msgid as msg1, msgs2.msgid as msg2, msgs2.content.date, p.userName
FROM profile.messages msgs2, profile.messages msgs1, profile p
WHERE msgs1.uid = p.uid and
      msgs2.uid = p.uid and
      msgs1.content.receivers[] >any msgs2.content.sender and
      msgs2.content.date > "2024-07-06"

#11 lauren
#21 john
#22 jin
#32 tim
#42 aswini
#70 george
