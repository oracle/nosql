#expression returns week using week()function for non json array 
SELECT id,week(t.ats9[0])as ats0,week(t.ats9[1])as ats1,week(t.ats9[2])as ats2,week(t.ats9[3])as ats3,week(t.ats9[4])as ats4,week(t.ats9[5])as ats5,week(t.ats9[6])as ats6,week(t.ats9[7])as ats7 ,week(t.ats9[8])as ats8 ,week(t.ats9[9])as ats9 FROM Extract t
