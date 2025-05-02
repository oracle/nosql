compiled-query-plan

{
"query file" : "json_idx/q/in03.q",
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
      "index used" : "idx_children_both",
      "covering index" : true,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {"info.children.keys()":"Anna","info.children.values().age":3},
          "range conditions" : { "info.children.values().school" : { "end value" : "sch_1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.children.keys()":"Mark","info.children.values().age":3},
          "range conditions" : { "info.children.values().school" : { "end value" : "sch_1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.children.keys()":"Anna","info.children.values().age":5},
          "range conditions" : { "info.children.values().school" : { "end value" : "sch_1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.children.keys()":"Anna","info.children.values().age":10},
          "range conditions" : { "info.children.values().school" : { "end value" : "sch_1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.children.keys()":"Mark","info.children.values().age":5},
          "range conditions" : { "info.children.values().school" : { "end value" : "sch_1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.children.keys()":"Mark","info.children.values().age":10},
          "range conditions" : { "info.children.values().school" : { "end value" : "sch_1", "end inclusive" : true } }
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