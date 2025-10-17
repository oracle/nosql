compiled-query-plan
{
"query file" : "row_metadata/q/aq02.q",
"plan" : 
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
      "row variable" : "$f",
      "index used" : "idx_state_areacode_age",
      "covering index" : true,
      "index row variable" : "$f_idx",
      "index scans" : [
        {
          "equality conditions" : {"row_metadata().address.state":"CA","row_metadata().address.phones[].areacode":650},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$f_idx",
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
            "variable" : "$f_idx"
          }
        }
      }
    ]
  }
}
}
