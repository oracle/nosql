#cot of array types and array in json
select t.numArr, cot(t.numArr) as cotnumArr,
       t.num2DArr, cot(t.num2DArr) as cotnum2DArr,
       t.num3DArr, cot(t.num3DArr) as cotnum3DArr,
       t.douArr, cot(t.numArr) as cotdouArr,
       t.dou2DArr, cot(t.num2DArr) as cotdou2DArr,
       t.dou3DArr, cot(t.num3DArr) as cotdou3DArr,
                     t.doc.numArr as docnumArr, cot(t.doc.numArr) as cotdocnumArr,
                     t.doc.num2DArr as docnum2DArr, cot(t.doc.num2DArr) as cotdocnum2DArr,
                     t.doc.num3DArr as docnum3DArr, cot(t.doc.num3DArr) as cotdocnum3DArr,
                     t.doc.douArr as docdouArr, cot(t.doc.numArr) as cotdocdouArr,
                     t.doc.dou2DArr as docdou2DArr, cot(t.doc.num2DArr) as cotdocdou2DArr,
                     t.doc.dou3DArr as  docdou3DArr, cot(t.doc.num3DArr) as cotdocdou3DArr
 from functional_test t where id=1