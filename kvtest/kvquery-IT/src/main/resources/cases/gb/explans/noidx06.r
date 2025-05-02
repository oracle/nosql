compiled-query-plan

{
"query file" : "gb/q/noidx06.q",
"plan" : 
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
          "target table" : "Foo",
          "row variable" : "$$f",
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
        "FROM variable" : "$$f",
        "SELECT expressions" : [
          {
            "field name" : "a",
            "field expression" : 
            {
              "iterator kind" : "PROMOTE",
              "target type" : "Any",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "a",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "mixed",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f"
                  }
                }
              }
            }
          },
          {
            "field name" : "cnt",
            "field expression" : 
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          },
          {
            "field name" : "min",
            "field expression" : 
            {
              "iterator kind" : "FN_SEQ_MIN",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "x",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "mixed",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f"
                  }
                }
              }
            }
          },
          {
            "field name" : "max",
            "field expression" : 
            {
              "iterator kind" : "FN_SEQ_MAX",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "x",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "mixed",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f"
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
          "field name" : "a",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-1"
          }
        }
      ],
      "aggregate functions" : [
        {
          "iterator kind" : "FUNC_COUNT_STAR"
        },
        {
          "iterator kind" : "FN_MIN",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "min",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-1"
            }
          }
        },
        {
          "iterator kind" : "FN_MAX",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "max",
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
      "field name" : "a",
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
        "field name" : "cnt",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-2"
        }
      }
    },
    {
      "iterator kind" : "FN_MIN",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "min",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-2"
        }
      }
    },
    {
      "iterator kind" : "FN_MAX",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "max",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-2"
        }
      }
    }
  ]
}
}