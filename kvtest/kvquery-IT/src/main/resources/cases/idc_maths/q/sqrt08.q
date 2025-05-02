#sqrt of array types and array in json
select t.numArr, sqrt(t.numArr) as sqrtnumArr,
       t.num2DArr, sqrt(t.num2DArr) as sqrtnum2DArr,
       t.num3DArr, sqrt(t.num3DArr) as sqrtnum3DArr,
       t.douArr, sqrt(t.numArr) as sqrtdouArr,
       t.dou2DArr, sqrt(t.num2DArr) as sqrtdou2DArr,
       t.dou3DArr, sqrt(t.num3DArr) as sqrtdou3DArr,
                     t.doc.numArr as docnumArr, sqrt(t.doc.numArr) as sqrtdocnumArr,
                     t.doc.num2DArr as docnum2DArr, sqrt(t.doc.num2DArr) as sqrtdocnum2DArr,
                     t.doc.num3DArr as docnum3DArr, sqrt(t.doc.num3DArr) as sqrtdocnum3DArr,
                     t.doc.douArr as docdouArr, sqrt(t.doc.numArr) as sqrtdocdouArr,
                     t.doc.dou2DArr as docdou2DArr, sqrt(t.doc.num2DArr) as sqrtdocdou2DArr,
                     t.doc.dou3DArr as  docdou3DArr, sqrt(t.doc.num3DArr) as sqrtdocdou3DArr
 from functional_test t where id=1