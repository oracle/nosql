compiled-query-plan

{
"query file" : "queryspec/q/q04.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "distinct by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Users2",
      "row variable" : "$$u",
      "index used" : "midx7",
      "covering index" : true,
      "index row variable" : "$$u_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "ANY_EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "matrixes[].matrix[].values()[].bar",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$u_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 2
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$u_idx",
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
            "variable" : "$$u_idx"
          }
        }
      }
    ]
  }
}
}