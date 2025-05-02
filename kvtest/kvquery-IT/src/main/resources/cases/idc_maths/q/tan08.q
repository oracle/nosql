#tan of array types and array in json
select t.numArr, tan(t.numArr) as tannumArr,
       t.num2DArr, tan(t.num2DArr) as tannum2DArr,
       t.num3DArr, tan(t.num3DArr) as tannum3DArr,
       t.douArr, tan(t.numArr) as tandouArr,
       t.dou2DArr, tan(t.num2DArr) as tandou2DArr,
       t.dou3DArr, tan(t.num3DArr) as tandou3DArr,
                     t.doc.numArr as docnumArr, tan(t.doc.numArr) as tandocnumArr,
                     t.doc.num2DArr as docnum2DArr, tan(t.doc.num2DArr) as tandocnum2DArr,
                     t.doc.num3DArr as docnum3DArr, tan(t.doc.num3DArr) as tandocnum3DArr,
                     t.doc.douArr as docdouArr, tan(t.doc.numArr) as tandocdouArr,
                     t.doc.dou2DArr as docdou2DArr, tan(t.doc.num2DArr) as tandocdou2DArr,
                     t.doc.dou3DArr as  docdou3DArr, tan(t.doc.num3DArr) as tandocdou3DArr
 from functional_test t where id=1