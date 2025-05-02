#expression returns isoweek using isoweek()function for non json array 
SELECT id,isoweek(t.ats9[0])as ats0,isoweek(t.ats9[1])as ats1,isoweek(t.ats9[2])as ats2,isoweek(t.ats9[3])as ats3,isoweek(t.ats9[4])as ats4,isoweek(t.ats9[5])as ats5,isoweek(t.ats9[6])as ats6,isoweek(t.ats9[7])as ats7 ,isoweek(t.ats9[8])as ats8 ,isoweek(t.ats9[9])as ats9 FROM Extract t
