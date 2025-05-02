#expression returns year using year()function for non json map
SELECT id,year(t.mts9.k0)as mts0,year(t.mts9.k1)as mts1,year(t.mts9.k2)as mts2,year(t.mts9.k3)as mts3,year(t.mts9.k4)as mts4,year(t.mts9.k5)as mts5,year(t.mts9.k6)as mts6,year(t.mts9.k7)as mts7 ,year(t.mts9.k8)as mts8 ,year(t.mts9.k9)as mts9 FROM Extract t
