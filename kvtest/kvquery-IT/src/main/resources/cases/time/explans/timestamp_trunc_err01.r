compiled-query-plan

{
"query file" : "time/q/timestamp_trunc_err01.q",
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
      "target table" : "roundtest",
      "row variable" : "$$roundtest",
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
    "FROM variable" : "$$roundtest",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "t0",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$roundtest"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "iyear"
            }
          ]
        }
      }
    ]
  }
}
}