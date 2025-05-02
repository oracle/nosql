SELECT msgs.uid, count(*) as cnt
FROM profile.messages msgs, profile p
WHERE msgs.uid = p.uid and p.age > 50
group by msgs.uid
