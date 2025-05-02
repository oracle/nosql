compiled-query-plan

{
"query file" : "time/q/arith_add03.q",
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
        "field name" : "tm9",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "tm9",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$arithtest"
          }
        }
      },
      {
        "field name" : "T3_P1D",
        "field expression" : 
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
                "field name" : "tm3",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$arithtest"
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "1 day"
            }
          ]
        }
      },
      {
        "field name" : "T9_P1D",
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
              "value" : "1 day"
            }
          ]
        }
      },
      {
        "field name" : "T3_P1MS",
        "field expression" : 
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
                "field name" : "tm3",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$arithtest"
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "1 millisecond"
            }
          ]
        }
      },
      {
        "field name" : "T9_P1NS",
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