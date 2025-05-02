compiled-query-plan

{
"query file" : "rowprops/q/isize09.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SORT",
    "order by fields at positions" : [ 2 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "GROUP",
        "input variable" : "$gb-3",
        "input iterator" :
        {
          "iterator kind" : "RECEIVE",
          "distribution kind" : "ALL_SHARDS",
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
                "row variable" : "$f",
                "index used" : "idx_city_phones",
                "covering index" : false,
                "index scans" : [
                  {
                    "equality conditions" : {},
                    "range conditions" : {}
                  }
                ],
                "position in join" : 0
              },
              "FROM variable" : "$f",
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
                      "variable" : "$f"
                    }
                  }
                },
                {
                  "field name" : "isize",
                  "field expression" : 
                  {
                    "iterator kind" : "FUNC_MKINDEX_STORAGE_SIZE",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$f"
                    }
                  }
                },
                {
                  "field name" : "firstName",
                  "field expression" : 
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "firstName",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$f"
                    }
                  }
                }
              ]
            },
            "grouping expressions" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "id",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$gb-1"
                }
              },
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "firstName",
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
                  "field name" : "isize",
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
            "field name" : "id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-3"
            }
          },
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "firstName",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-3"
            }
          }
        ],
        "aggregate functions" : [
          {
            "iterator kind" : "FUNC_SUM",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "isize",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$gb-3"
              }
            }
          }
        ]
      },
      "FROM variable" : "$from-2",
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
              "variable" : "$from-2"
            }
          }
        },
        {
          "field name" : "isize",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "isize",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-2"
            }
          }
        },
        {
          "field name" : "firstName",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "firstName",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-2"
            }
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-0",
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
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "isize",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "isize",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "firstName",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "firstName",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    }
  ],
  "OFFSET" :
  {
    "iterator kind" : "CONST",
    "value" : 2
  },
  "LIMIT" :
  {
    "iterator kind" : "CONST",
    "value" : 3
  }
}
}