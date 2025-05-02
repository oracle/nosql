compiled-query-plan

{
"query file" : "nested_arrays/q/unnest08.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "order by fields at positions" : [ 0 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Bar",
        "row variable" : "$t",
        "index used" : "idx_state_areacode_kind",
        "covering index" : true,
        "index row variable" : "$t_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : { "info.addresses[].state" : { "start value" : "C", "start inclusive" : false } }
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$t_idx",
      "GROUP BY" : "Grouping by the first expression in the SELECT list",
      "SELECT expressions" : [
        {
          "field name" : "state",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info.addresses[].state",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$t_idx"
            }
          }
        },
        {
          "field name" : "cnt",
          "field expression" : 
          {
            "iterator kind" : "FN_COUNT",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info.addresses[].phones[][][].kind",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$t_idx"
              }
            }
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "Grouping by the first expression in the SELECT list",
  "SELECT expressions" : [
    {
      "field name" : "state",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "state",
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