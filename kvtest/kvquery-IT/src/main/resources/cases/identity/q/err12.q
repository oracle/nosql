CREATE TABLE Foo(
    id LONG GENERATED ALWAYS AS IDENTITY (CACHE 5000 CACHE 6000),
    name STRING,
    PRIMARY KEY (id)
)
