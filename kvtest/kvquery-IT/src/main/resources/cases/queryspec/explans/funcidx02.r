compiled-query-plan

{
"query file" : "queryspec/q/funcidx02.q",
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
      "target table" : "Users",
      "row variable" : "$$u",
      "index used" : "idx_year_month",
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
        "iterator kind" : "EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "substring#address.startDate@,5,2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$u_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : "04"
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