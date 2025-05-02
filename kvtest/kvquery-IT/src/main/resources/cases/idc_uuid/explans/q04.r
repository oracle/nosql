compiled-query-plan

{
"query file" : "idc_uuid/q/q04.q",
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
        "target table" : "bar1",
        "row variable" : "$$f",
        "index used" : "idx_bar1_str_uid2",
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
      "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
      "SELECT expressions" : [
        {
          "field name" : "firstName",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "firstName",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f_idx"
            }
          }
        },
        {
          "field name" : "uid2",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "uid2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f_idx"
            }
          }
        }
      ],
      "LIMIT" :
      {
        "iterator kind" : "CONST",
        "value" : 2
      }
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
  "SELECT expressions" : [
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
    },
    {
      "field name" : "uid2",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "uid2",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    }
  ],
  "LIMIT" :
  {
    "iterator kind" : "CONST",
    "value" : 2
  }
}
}