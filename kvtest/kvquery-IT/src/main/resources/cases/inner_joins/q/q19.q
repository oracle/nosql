SELECT msgs1.msgid as msg1, msgs2.msgid as msg2, msgs2.content.date, p.userName
FROM profile.messages msgs2, profile.messages msgs1, profile p
WHERE msgs1.uid = p.uid and
      msgs2.uid = p.uid and
      exists msgs1.content.receivers[msgs2.content.sender < $element and
                                     $element < cast(msgs2.content.receivers[0] as string) ] and
      msgs2.content.date > "2024-07-06"

#11 lauren - sam
#21 john - george
#22 jin - rubin
#32 tim - rubin
#42 aswini - george
#70 george - markos
