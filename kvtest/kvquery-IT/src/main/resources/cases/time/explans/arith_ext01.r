compiled-query-plan

{
"query file" : "time/q/arith_ext01.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
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
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$arithtest",
    "WHERE" : 
    {
      "iterator kind" : "GREATER_THAN",
      "left operand" :
      {
        "iterator kind" : "FN_TIMESTAMP_ADD",
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
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$dur1"
          }
        ]
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : "2021-12-04T00:00:00.000000000Z"
      }
    },
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$arithtest"
          }
        }
      }
    ]
  }
}
}