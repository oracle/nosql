compiled-query-plan

{
"query file" : "json_idx/q/ext_untyped08.q",
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
      "target table" : "Bar",
      "row variable" : "$$b",
      "index used" : "idx_state_city_age",
      "covering index" : true,
      "index row variable" : "$$b_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "info.address.state" : { "start value" : 0, "start inclusive" : false, "end value" : 0, "end inclusive" : false } }
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$state1"
        },
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$state3"
        }
      ],
      "map of key bind expressions" : [
        [ 0, 1 ]
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
      }
    ]
  }
}
}