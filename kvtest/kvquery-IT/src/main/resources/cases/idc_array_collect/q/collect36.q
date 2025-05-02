#parent-child table. For each user select their gender,age and list of mails sent.

select a.userInfo.gender,c.userId,array_collect(c.messageId) as messages,count(*) as count from nested tables(emails.folder.message c ancestors(emails a, emails.folder b)) where b.name='Sent' group by a.userInfo.gender,c.userId
