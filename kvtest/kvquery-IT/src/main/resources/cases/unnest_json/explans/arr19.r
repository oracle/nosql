compiled-query-plan

{
"query file" : "unnest_json/q/arr19.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "order by fields at positions" : [ 0, 1 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Foo",
        "row variable" : "$$t",
        "index used" : "idx_areacode_kind",
        "covering index" : true,
        "index row variable" : "$$t_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$t_idx",
      "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
      "SELECT expressions" : [
        {
          "field name" : "areacode",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info.address.phones[].areacode",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t_idx"
            }
          }
        },
        {
          "field name" : "kind",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info.address.phones[].kind",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t_idx"
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
  "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
  "SELECT expressions" : [
    {
      "field name" : "areacode",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "areacode",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "kind",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "kind",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
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