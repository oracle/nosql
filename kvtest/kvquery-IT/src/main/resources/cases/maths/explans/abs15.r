compiled-query-plan

{
"query file" : "maths/q/abs15.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "math_test",
      "row variable" : "$$math_test",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {"id":1},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$math_test",
    "SELECT expressions" : [
      {
        "field name" : "abs1",
        "field expression" : 
        {
          "iterator kind" : "ABS",
          "input iterators" : [
            {
              "iterator kind" : "ARITHMETIC_NEGATION",
              "input iterator" :
              {
                "iterator kind" : "PI",
                "input iterators" : [

                ]
              }
            }
          ]
        }
      },
      {
        "field name" : "abs2",
        "field expression" : 
        {
          "iterator kind" : "ABS",
          "input iterators" : [
            {
              "iterator kind" : "ARITHMETIC_NEGATION",
              "input iterator" :
              {
                "iterator kind" : "E",
                "input iterators" : [

                ]
              }
            }
          ]
        }
      }
    ]
  }
}
}