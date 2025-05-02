compiled-query-plan

{
"query file" : "nested_arrays/q/unnest06.q",
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
      "index filtering predicate" :
      {
        "iterator kind" : "EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info.addresses[].phones[][][].areacode",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$t_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 400
        }
      },
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