compiled-query-plan

{
"query file" : "time/q/arith_err_add01.q",
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
          "equality conditions" : {"id":2},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$arithtest",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_ADD",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "tm9",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$arithtest"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "1 nanosecond"
            }
          ]
        }
      }
    ]
  }
}
}