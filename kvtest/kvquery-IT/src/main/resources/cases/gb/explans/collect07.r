compiled-query-plan

{
"query file" : "gb/q/collect07.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Foo",
        "row variable" : "$$f",
        "index used" : "idx_acc_year_prodcat",
        "covering index" : true,
        "index row variable" : "$$f_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$f_idx",
      "GROUP BY" : "No grouping expressions",
      "SELECT expressions" : [
        {
          "field name" : "collect",
          "field expression" : 
          {
            "iterator kind" : "FUNC_COLLECT",
            "distinct" : false,
            "input iterator" :
            {
              "iterator kind" : "MAP_CONSTRUCTOR",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : "id1"
                },
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "#id1",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f_idx"
                  }
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "id2"
                },
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "#id2",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f_idx"
                  }
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "id3"
                },
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "#id3",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f_idx"
                  }
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "prodcat"
                },
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "xact.prodcat",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f_idx"
                  }
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "year"
                },
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "xact.year",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f_idx"
                  }
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "str1"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "str2"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "2xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "str3"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "3xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "str4"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "4xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                }
              ]
            }
          }
        },
        {
          "field name" : "cnt",
          "field expression" : 
          {
            "iterator kind" : "FUNC_COUNT_STAR"
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "No grouping expressions",
  "SELECT expressions" : [
    {
      "field name" : "collect",
      "field expression" : 
      {
        "iterator kind" : "FUNC_COLLECT",
        "distinct" : false,
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "collect",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    },
    {
      "field name" : "cnt",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "cnt",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    }
  ]
}
}