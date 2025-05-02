compiled-query-plan

{
"query file" : "unnest_json/q/arr04.q",
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
      "row variable" : "$t",
      "index used" : "idx_state_areacode_kind",
      "covering index" : true,
      "index row variable" : "$t_idx",
      "index scans" : [
        {
          "equality conditions" : {"info.address.state":"CA","info.address.phones[].areacode":650},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.address.state":"CA","info.address.phones[].areacode":408},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$t_idx",
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
            "variable" : "$t_idx"
          }
        }
      }
    ]
  }
}
}