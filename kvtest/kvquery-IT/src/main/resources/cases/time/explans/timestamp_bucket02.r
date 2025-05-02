compiled-query-plan

{
"query file" : "time/q/timestamp_bucket02.q",
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
        "field name" : "b_2weeks",
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
              "value" : "2 weeks"
            },
            {
              "iterator kind" : "CONST",
              "value" : "2021-01-01"
            }
          ]
        }
      },
      {
        "field name" : "b_5days",
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
              "value" : "5 days"
            },
            {
              "iterator kind" : "CONST",
              "value" : "2021-02-01"
            }
          ]
        }
      },
      {
        "field name" : "b_6hours",
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
              "value" : "6 hours"
            },
            {
              "iterator kind" : "CONST",
              "value" : "2021-02-28T01"
            }
          ]
        }
      },
      {
        "field name" : "b_7mins",
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
              "value" : "7 minutes"
            },
            {
              "iterator kind" : "CONST",
              "value" : "2021-03-01T23:30:00.999"
            }
          ]
        }
      },
      {
        "field name" : "b_18secs",
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
              "value" : "18 seconds"
            },
            {
              "iterator kind" : "CONST",
              "value" : "2021-02-01T00:00:30.000000001"
            }
          ]
        }
      }
    ]
  }
}
}