compiled-query-plan

{
"query file" : "time/q/timestamp_bucket01.q",
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
      "row variable" : "$$t",
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
    "FROM variable" : "$$t",
    "SELECT expressions" : [
      {
        "field name" : "t0",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "t0",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "t0_9secs",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_BUCKET",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "t0",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "9 SECOND"
            }
          ]
        }
      },
      {
        "field name" : "s9",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "s9",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "s9_5mins",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_BUCKET",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "s9",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "5 MINUTEs"
            }
          ]
        }
      },
      {
        "field name" : "t3",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "t3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "t3_2hours",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_BUCKET",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "t3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "2 hour"
            }
          ]
        }
      },
      {
        "field name" : "t3_day",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_BUCKET",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "t3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      },
      {
        "field name" : "s6",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "s6",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "doc",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      },
      {
        "field name" : "s6_5days",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_BUCKET",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "s6",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "doc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "5 Days"
            }
          ]
        }
      },
      {
        "field name" : "s6_2weeks",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_BUCKET",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "s6",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "doc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "2 weeks"
            }
          ]
        }
      }
    ]
  }
}
}