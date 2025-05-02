compiled-query-plan

{
"query file" : "json_idx/q/sort11.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SORT",
    "order by fields at positions" : [ 1, 0 ],
    "input iterator" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_SHARDS",
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "Bar",
          "row variable" : "$$b",
          "index used" : "idx_state_city_age",
          "covering index" : true,
          "index row variable" : "$$b_idx",
          "index scans" : [
            {
              "equality conditions" : {"info.address.state":"CA"},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$b_idx",
        "SELECT expressions" : [
          {
            "field name" : "id",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "#id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$b_idx"
              }
            }
          },
          {
            "field name" : "sort_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info.address.state",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$b_idx"
              }
            }
          }
        ]
      }
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
    }
  ]
}
}