compiled-query-plan

{
"query file" : "rowprops/q/jc_isize08.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 2 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "GROUP",
      "input variable" : "$gb-2",
      "input iterator" :
      {
        "iterator kind" : "RECEIVE",
        "distribution kind" : "ALL_SHARDS",
        "input iterator" :
        {
          "iterator kind" : "GROUP",
          "input variable" : "$gb-0",
          "input iterator" :
          {
            "iterator kind" : "SELECT",
            "FROM" :
            {
              "iterator kind" : "TABLE",
              "target table" : "Boo",
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
                "variable" : "$gb-0"
              }
            },
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "firstName",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$gb-0"
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
                  "variable" : "$gb-0"
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
            "variable" : "$gb-2"
          }
        },
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "firstName",
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
            "field name" : "isize",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-2"
            }
          }
        }
      ]
    },
    "FROM variable" : "$from-1",
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
            "variable" : "$from-1"
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
            "variable" : "$from-1"
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
            "variable" : "$from-1"
          }
        }
      }
    ]
  }
}
}