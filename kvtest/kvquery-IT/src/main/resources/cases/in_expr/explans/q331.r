compiled-query-plan

{
"query file" : "in_expr/q/q331.q",
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
      "target table" : "foo",
      "row variable" : "$$f",
      "index used" : "idx_phones",
      "covering index" : true,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {"info.phones[].num":0,"info.phones[].kind":""},
          "range conditions" : {}
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "ARRAY_FILTER",
          "input iterator" :
          {
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$arr26"
          }
        }
      ],
      "map of key bind expressions" : [
        [ 0, 0 ]
      ],
      "bind info for in3 operator" : [
        {
          "theNumComps" : 2,
          "thePushedComps" : [ 0, 1 ],
          "theIndexFieldPositions" : [ 0, 1 ]
         }
      ],
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