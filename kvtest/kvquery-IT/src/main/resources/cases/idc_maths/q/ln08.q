#ln of array types and array in json
select t.numArr, ln(t.numArr) as lnnumArr,
       t.num2DArr, ln(t.num2DArr) as lnnum2DArr,
       t.num3DArr, ln(t.num3DArr) as lnnum3DArr,
       t.douArr, ln(t.numArr) as lndouArr,
       t.dou2DArr, ln(t.num2DArr) as lndou2DArr,
       t.dou3DArr, ln(t.num3DArr) as lndou3DArr,
                     t.doc.numArr as docnumArr, ln(t.doc.numArr) as lndocnumArr,
                     t.doc.num2DArr as docnum2DArr, ln(t.doc.num2DArr) as lndocnum2DArr,
                     t.doc.num3DArr as docnum3DArr, ln(t.doc.num3DArr) as lndocnum3DArr,
                     t.doc.douArr as docdouArr, ln(t.doc.numArr) as lndocdouArr,
                     t.doc.dou2DArr as docdou2DArr, ln(t.doc.num2DArr) as lndocdou2DArr,
                     t.doc.dou3DArr as  docdou3DArr, ln(t.doc.num3DArr) as lndocdou3DArr
 from functional_test t where id=1