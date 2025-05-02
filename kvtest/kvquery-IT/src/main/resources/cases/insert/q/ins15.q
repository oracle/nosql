insert into foo f (id1, id2, rec1) values(
  100,
  1,
  {
    "rec2" : {
      "num" : 1e-500
    },
    "recinfo" : {
      "val" : -1e-500
    } 
  }
)
returning f.rec1.rec2.num, f.rec1.recinfo.val
