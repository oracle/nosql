compiled-query-plan

{
"query file" : "time/q/arith_diff01.q",
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
        "iterator kind" : "FN_TIMESTAMP_DIFF",
        "input iterators" : [
          {
            "iterator kind" : "CAST",
            "target type" : "Timestamp(9)",
            "quantifier" : "?",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "tm3",
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
              "iterator kind" : "FIELD_STEP",
              "field name" : "tm0",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$arithtest"
              }
            }
          }
        ]
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 0
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
      },
      {
        "field name" : "tm3",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "tm3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$arithtest"
          }
        }
      },
      {
        "field name" : "tm0",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "tm0",
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