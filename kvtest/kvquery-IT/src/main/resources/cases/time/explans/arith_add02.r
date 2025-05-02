compiled-query-plan

{
"query file" : "time/q/arith_add02.q",
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
        "field name" : "P12H",
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
              "value" : "12 hours"
            }
          ]
        }
      },
      {
        "field name" : "SUB_P300MI",
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
              "value" : "-300 minutes"
            }
          ]
        }
      },
      {
        "field name" : "SUB_P3600S500MS",
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
              "value" : "3600 seconds 500 milliseconds"
            }
          ]
        }
      },
      {
        "field name" : "SUB_P999MS999999NS",
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
              "value" : "- 999 milliseconds 999999 nanoseconds"
            }
          ]
        }
      },
      {
        "field name" : "P1H1MI1S1MS1NS",
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
              "value" : "1 hour 1 minute 1 second 1 milliseconds 1 nanoseconds"
            }
          ]
        }
      },
      {
        "field name" : "SUB_P1H1MI1S1MS1NS",
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
              "value" : "- 1 hour 1 minute 1 second 1 milliseconds 1 nanoseconds"
            }
          ]
        }
      }
    ]
  }
}
}