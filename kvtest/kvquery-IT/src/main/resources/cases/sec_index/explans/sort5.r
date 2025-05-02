compiled-query-plan

{
"query file" : "sec_index/q/sort5.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SORT",
    "order by fields at positions" : [ 1 ],
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
          "target table" : "Foo",
          "row variable" : "$$t",
          "index used" : "idx_state_city_age",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : { "address.state" : { "start value" : "MA", "start inclusive" : true } }
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$t",
        "SELECT expressions" : [
          {
            "field name" : "t",
            "field expression" : 
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          },
          {
            "field name" : "sort_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
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
      "field name" : "t",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "t",
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