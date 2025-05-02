compiled-query-plan

{
"query file" : "maths/q/asin03.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "math_test",
      "row variable" : "$$math_test",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {"id":1},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$math_test",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "ASIN",
            "input iterators" : [
              {
                "iterator kind" : "SIN",
                "input iterators" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : 0
                  }
                ]
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 0.0
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "ASIN",
                "input iterators" : [
                  {
                    "iterator kind" : "SIN",
                    "input iterators" : [
                      {
                        "iterator kind" : "MULTIPLY_DIVIDE",
                        "operations and operands" : [
                          {
                            "operation" : "*",
                            "operand" :
                            {
                              "iterator kind" : "PI",
                              "input iterators" : [

                              ]
                            }
                          },
                          {
                            "operation" : "/",
                            "operand" :
                            {
                              "iterator kind" : "CONST",
                              "value" : 6
                            }
                          }
                        ]
                      }
                    ]
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "MULTIPLY_DIVIDE",
                "operations and operands" : [
                  {
                    "operation" : "*",
                    "operand" :
                    {
                      "iterator kind" : "PI",
                      "input iterators" : [

                      ]
                    }
                  },
                  {
                    "operation" : "/",
                    "operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 6
                    }
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_3",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "ASIN",
                "input iterators" : [
                  {
                    "iterator kind" : "SIN",
                    "input iterators" : [
                      {
                        "iterator kind" : "MULTIPLY_DIVIDE",
                        "operations and operands" : [
                          {
                            "operation" : "*",
                            "operand" :
                            {
                              "iterator kind" : "PI",
                              "input iterators" : [

                              ]
                            }
                          },
                          {
                            "operation" : "/",
                            "operand" :
                            {
                              "iterator kind" : "CONST",
                              "value" : 4
                            }
                          }
                        ]
                      }
                    ]
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "MULTIPLY_DIVIDE",
                "operations and operands" : [
                  {
                    "operation" : "*",
                    "operand" :
                    {
                      "iterator kind" : "PI",
                      "input iterators" : [

                      ]
                    }
                  },
                  {
                    "operation" : "/",
                    "operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 4
                    }
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_4",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "ASIN",
                "input iterators" : [
                  {
                    "iterator kind" : "SIN",
                    "input iterators" : [
                      {
                        "iterator kind" : "MULTIPLY_DIVIDE",
                        "operations and operands" : [
                          {
                            "operation" : "*",
                            "operand" :
                            {
                              "iterator kind" : "PI",
                              "input iterators" : [

                              ]
                            }
                          },
                          {
                            "operation" : "/",
                            "operand" :
                            {
                              "iterator kind" : "CONST",
                              "value" : 3
                            }
                          }
                        ]
                      }
                    ]
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "MULTIPLY_DIVIDE",
                "operations and operands" : [
                  {
                    "operation" : "*",
                    "operand" :
                    {
                      "iterator kind" : "PI",
                      "input iterators" : [

                      ]
                    }
                  },
                  {
                    "operation" : "/",
                    "operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 3
                    }
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_5",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "ASIN",
                "input iterators" : [
                  {
                    "iterator kind" : "SIN",
                    "input iterators" : [
                      {
                        "iterator kind" : "MULTIPLY_DIVIDE",
                        "operations and operands" : [
                          {
                            "operation" : "*",
                            "operand" :
                            {
                              "iterator kind" : "PI",
                              "input iterators" : [

                              ]
                            }
                          },
                          {
                            "operation" : "/",
                            "operand" :
                            {
                              "iterator kind" : "CONST",
                              "value" : 2
                            }
                          }
                        ]
                      }
                    ]
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "MULTIPLY_DIVIDE",
                "operations and operands" : [
                  {
                    "operation" : "*",
                    "operand" :
                    {
                      "iterator kind" : "PI",
                      "input iterators" : [

                      ]
                    }
                  },
                  {
                    "operation" : "/",
                    "operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 2
                    }
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_6",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "ASIN",
                "input iterators" : [
                  {
                    "iterator kind" : "SIN",
                    "input iterators" : [
                      {
                        "iterator kind" : "MULTIPLY_DIVIDE",
                        "operations and operands" : [
                          {
                            "operation" : "*",
                            "operand" :
                            {
                              "iterator kind" : "ARITHMETIC_NEGATION",
                              "input iterator" :
                              {
                                "iterator kind" : "PI",
                                "input iterators" : [

                                ]
                              }
                            }
                          },
                          {
                            "operation" : "/",
                            "operand" :
                            {
                              "iterator kind" : "CONST",
                              "value" : 6
                            }
                          }
                        ]
                      }
                    ]
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "MULTIPLY_DIVIDE",
                "operations and operands" : [
                  {
                    "operation" : "*",
                    "operand" :
                    {
                      "iterator kind" : "ARITHMETIC_NEGATION",
                      "input iterator" :
                      {
                        "iterator kind" : "PI",
                        "input iterators" : [

                        ]
                      }
                    }
                  },
                  {
                    "operation" : "/",
                    "operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 6
                    }
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_7",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "ASIN",
                "input iterators" : [
                  {
                    "iterator kind" : "SIN",
                    "input iterators" : [
                      {
                        "iterator kind" : "MULTIPLY_DIVIDE",
                        "operations and operands" : [
                          {
                            "operation" : "*",
                            "operand" :
                            {
                              "iterator kind" : "ARITHMETIC_NEGATION",
                              "input iterator" :
                              {
                                "iterator kind" : "PI",
                                "input iterators" : [

                                ]
                              }
                            }
                          },
                          {
                            "operation" : "/",
                            "operand" :
                            {
                              "iterator kind" : "CONST",
                              "value" : 4
                            }
                          }
                        ]
                      }
                    ]
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "MULTIPLY_DIVIDE",
                "operations and operands" : [
                  {
                    "operation" : "*",
                    "operand" :
                    {
                      "iterator kind" : "ARITHMETIC_NEGATION",
                      "input iterator" :
                      {
                        "iterator kind" : "PI",
                        "input iterators" : [

                        ]
                      }
                    }
                  },
                  {
                    "operation" : "/",
                    "operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 4
                    }
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_8",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "ASIN",
                "input iterators" : [
                  {
                    "iterator kind" : "SIN",
                    "input iterators" : [
                      {
                        "iterator kind" : "MULTIPLY_DIVIDE",
                        "operations and operands" : [
                          {
                            "operation" : "*",
                            "operand" :
                            {
                              "iterator kind" : "ARITHMETIC_NEGATION",
                              "input iterator" :
                              {
                                "iterator kind" : "PI",
                                "input iterators" : [

                                ]
                              }
                            }
                          },
                          {
                            "operation" : "/",
                            "operand" :
                            {
                              "iterator kind" : "CONST",
                              "value" : 3
                            }
                          }
                        ]
                      }
                    ]
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "MULTIPLY_DIVIDE",
                "operations and operands" : [
                  {
                    "operation" : "*",
                    "operand" :
                    {
                      "iterator kind" : "ARITHMETIC_NEGATION",
                      "input iterator" :
                      {
                        "iterator kind" : "PI",
                        "input iterators" : [

                        ]
                      }
                    }
                  },
                  {
                    "operation" : "/",
                    "operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 3
                    }
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_9",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "ASIN",
                "input iterators" : [
                  {
                    "iterator kind" : "SIN",
                    "input iterators" : [
                      {
                        "iterator kind" : "MULTIPLY_DIVIDE",
                        "operations and operands" : [
                          {
                            "operation" : "*",
                            "operand" :
                            {
                              "iterator kind" : "ARITHMETIC_NEGATION",
                              "input iterator" :
                              {
                                "iterator kind" : "PI",
                                "input iterators" : [

                                ]
                              }
                            }
                          },
                          {
                            "operation" : "/",
                            "operand" :
                            {
                              "iterator kind" : "CONST",
                              "value" : 2
                            }
                          }
                        ]
                      }
                    ]
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "MULTIPLY_DIVIDE",
                "operations and operands" : [
                  {
                    "operation" : "*",
                    "operand" :
                    {
                      "iterator kind" : "ARITHMETIC_NEGATION",
                      "input iterator" :
                      {
                        "iterator kind" : "PI",
                        "input iterators" : [

                        ]
                      }
                    }
                  },
                  {
                    "operation" : "/",
                    "operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 2
                    }
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            ]
          }
        }
      }
    ]
  }
}
}