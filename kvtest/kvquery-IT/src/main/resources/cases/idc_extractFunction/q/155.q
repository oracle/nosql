#expression returns hour,minute,second and has logical operator OR using timestamp function for json array using cast and non json array using cast
SELECT id,hour(cast(t.json.at[2] as timestamp)),minute(cast(t.json.at[2] as timestamp ))as jats2,second(t.ats9[2])  FROM Extract t WHERE id=2 OR id=10 OR exists t.json
