compiled-query-plan

{
"query file" : "rowprops/q/isize13.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-1",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "GROUP",
      "input variable" : "$gb-4",
      "input iterator" :
      {
        "iterator kind" : "RECEIVE",
        "distribution kind" : "ALL_SHARDS",
        "input iterator" :
        {
          "iterator kind" : "GROUP",
          "input variable" : "$gb-2",
          "input iterator" :
          {
            "iterator kind" : "SELECT",
            "FROM" :
            {
              "iterator kind" : "TABLE",
              "target table" : "Foo",
              "row variable" : "$f",
              "index used" : "idx_city_phones",
              "covering index" : true,
              "index row variable" : "$f_idx",
              "index scans" : [
                {
                  "equality conditions" : {},
                  "range conditions" : {}
                }
              ],
              "position in join" : 0
            },
            "FROM variable" : "$f_idx",
            "SELECT expressions" : [
              {
                "field name" : "shard",
                "field expression" : 
                {
                  "iterator kind" : "FUNC_SHARD",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$f_idx"
                  }
                }
              },
              {
                "field name" : "index_size",
                "field expression" : 
                {
                  "iterator kind" : "FUNC_MKINDEX_STORAGE_SIZE",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$f_idx"
                  }
                }
              },
              {
                "field name" : "id_gen",
                "field expression" : 
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "#id",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$f_idx"
                  }
                }
              }
            ]
          },
          "grouping expressions" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "shard",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$gb-2"
              }
            },
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "id_gen",
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
                "field name" : "index_size",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$gb-2"
                }
              }
            }
          ]
        }
      },
      "grouping expressions" : [
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "shard",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-4"
          }
        },
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id_gen",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-4"
          }
        }
      ],
      "aggregate functions" : [
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "index_size",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-4"
            }
          }
        }
      ]
    },
    "FROM variable" : "$from-3",
    "SELECT expressions" : [
      {
        "field name" : "shard",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "shard",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-3"
          }
        }
      },
      {
        "field name" : "index_size",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "index_size",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-3"
          }
        }
      }
    ]
  },
  "grouping expressions" : [
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "shard",
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
        "field name" : "index_size",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-1"
        }
      }
    }
  ]
}
}