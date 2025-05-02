compiled-query-plan

{
"query file" : "rowprops/q/ext_part08.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "partition id bind expressions" : [
    {
      "iterator kind" : "ADD_SUBTRACT",
      "operations and operands" : [
        {
          "operation" : "+",
          "operand" :
          {
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$p3"
          }
        },
        {
          "operation" : "+",
          "operand" :
          {
            "iterator kind" : "CONST",
            "value" : 1
          }
        }
      ]
    },
    null
  ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Foo",
      "row variable" : "$f",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$f",
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
            "variable" : "$f"
          }
        }
      }
    ]
  }
}
}