compiled-query-plan

{
"query file" : "time/q/arith_ext02.q",
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
      "target table" : "arithtest",
      "row variable" : "$$arithtest",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":0},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$arithtest",
    "SELECT expressions" : [
      {
        "field name" : "DIFF",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_DIFF",
          "input iterators" : [
            {
              "iterator kind" : "CAST",
              "target type" : "Timestamp(9)",
              "quantifier" : "?",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "tm0",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$arithtest"
                }
              }
            },
            {
              "iterator kind" : "CAST",
              "target type" : "Timestamp(9)",
              "quantifier" : "?",
              "input iterator" :
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$tm1"
              }
            }
          ]
        }
      }
    ]
  }
}
}