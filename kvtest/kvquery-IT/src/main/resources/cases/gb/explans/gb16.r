compiled-query-plan

{
"query file" : "gb/q/gb16.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "GROUP",
    "input variable" : "$gb-2",
    "input iterator" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_PARTITIONS",
      "input iterator" :
      {
        "iterator kind" : "GROUP",
        "input variable" : "$gb-1",
        "input iterator" :
        {
          "iterator kind" : "SELECT",
          "FROM" :
          {
            "iterator kind" : "TABLE",
            "target table" : "XYZ",
            "row variable" : "$$b",
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
          "FROM variable" : "$$b",
          "SELECT expressions" : [
            {
              "field name" : "gb-0",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "s",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$b"
                }
              }
            },
            {
              "field name" : "aggr-1",
              "field expression" : 
              {
                "iterator kind" : "FN_SEQ_COUNT_I",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "i",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "j",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$b"
                    }
                  }
                }
              }
            }
          ]
        },
        "grouping expressions" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "gb-0",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-1"
            }
          }
        ],
        "aggregate functions" : [
          {
            "iterator kind" : "FUNC_SUM",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "aggr-1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$gb-1"
              }
            }
          }
        ]
      }
    },
    "grouping expressions" : [
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "gb-0",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-2"
        }
      }
    ],
    "aggregate functions" : [
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "aggr-1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-2"
          }
        }
      }
    ]
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "Column_1",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "aggr-1",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    }
  ]
}
}