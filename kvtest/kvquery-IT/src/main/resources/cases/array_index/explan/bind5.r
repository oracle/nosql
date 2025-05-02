compiled-query-plan

{
"query file" : "array_index/q/bind5.q",
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
      "row variable" : "$$t",
      "index used" : "idx_a_c_f",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"rec.a":10},
          "range conditions" : { "rec.c[].ca" : { "start value" : 0, "start inclusive" : true } }
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$ext5_1"
        }
      ],
      "map of key bind expressions" : [
        [ -1, 0, -1 ]
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "WHERE" : 
    {
      "iterator kind" : "ANY_GREATER_THAN",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "ca",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "c",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "rec",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 6
      }
    },
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
            "variable" : "$$t"
          }
        }
      }
    ]
  }
}
}