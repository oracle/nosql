compiled-query-plan

{
"query file" : "json_idx/q/ex03.q",
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
      "target table" : "Foo",
      "row variable" : "$$f",
      "index used" : "idx_areacode_kind",
      "covering index" : true,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "OP_EXISTS",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info.address.phones[].kind",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f_idx"
          }
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$f_idx",
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
            "variable" : "$$f_idx"
          }
        }
      }
    ]
  }
}
}