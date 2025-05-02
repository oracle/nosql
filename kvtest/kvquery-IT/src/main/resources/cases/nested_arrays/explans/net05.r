compiled-query-plan

{
"query file" : "nested_arrays/q/net05.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 2 ],
  "input iterator" :
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
            "target table" : "netflix",
            "row variable" : "$$n",
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
          "FROM variable" : "$$n",
          "FROM" :
          {
            "iterator kind" : "ARRAY_FILTER",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "contentStreamed",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "value",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$n"
                }
              }
            }
          },
          "FROM variable" : "$shows",
          "SELECT expressions" : [
            {
              "field name" : "showId",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "showId",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$shows"
                }
              }
            },
            {
              "field name" : "showName",
              "field expression" : 
              {
                "iterator kind" : "PROMOTE",
                "target type" : "Any",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "showName",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$shows"
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
            }
          ]
        },
        "grouping expressions" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "showId",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-1"
            }
          },
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "showName",
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
          }
        ]
      }
    },
    "grouping expressions" : [
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "showId",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-2"
        }
      },
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "showName",
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
      }
    ]
  }
}
}