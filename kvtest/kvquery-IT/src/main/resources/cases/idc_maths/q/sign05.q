#sign of array types and array in json
select t.numArr, sign(t.numArr) as signnumArr,
       t.num2DArr, sign(t.num2DArr) as signnum2DArr,
       t.num3DArr, sign(t.num3DArr) as signnum3DArr,
       t.douArr, sign(t.numArr) as signdouArr,
       t.dou2DArr, sign(t.num2DArr) as signdou2DArr,
       t.dou3DArr, sign(t.num3DArr) as signdou3DArr,
                     t.doc.numArr as docnumArr, sign(t.doc.numArr) as signdocnumArr,
                     t.doc.num2DArr as docnum2DArr, sign(t.doc.num2DArr) as signdocnum2DArr,
                     t.doc.num3DArr as docnum3DArr, sign(t.doc.num3DArr) as signdocnum3DArr,
                     t.doc.douArr as docdouArr, sign(t.doc.numArr) as signdocdouArr,
                     t.doc.dou2DArr as docdou2DArr, sign(t.doc.num2DArr) as signdocdou2DArr,
                     t.doc.dou3DArr as  docdou3DArr, sign(t.doc.num3DArr) as signdocdou3DArr
 from functional_test t where id=1