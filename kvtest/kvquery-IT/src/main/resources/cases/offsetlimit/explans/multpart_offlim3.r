compiled-query-plan

{
"query file" : "offsetlimit/q/multpart_offlim3.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_PARTITIONS",
    "order by fields at positions" : [ 0, 1 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Foo",
        "row variable" : "$$Foo",
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
      "FROM variable" : "$$Foo",
      "SELECT expressions" : [
        {
          "field name" : "id1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Foo"
            }
          }
        },
        {
          "field name" : "id2",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Foo"
            }
          }
        },
        {
          "field name" : "id3",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Foo"
            }
          }
        },
        {
          "field name" : "id4",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id4",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Foo"
            }
          }
        }
      ],
      "LIMIT" :
      {
        "iterator kind" : "ADD_SUBTRACT",
        "operations and operands" : [
          {
            "operation" : "+",
            "operand" :
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          },
          {
            "operation" : "+",
            "operand" :
            {
              "iterator kind" : "CONST",
              "value" : 15
            }
          }
        ]
      }
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "$from-0",
      "field expression" : 
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$from-0"
      }
    }
  ],
  "OFFSET" :
  {
    "iterator kind" : "CONST",
    "value" : 3
  },
  "LIMIT" :
  {
    "iterator kind" : "CONST",
    "value" : 15
  }
}
}