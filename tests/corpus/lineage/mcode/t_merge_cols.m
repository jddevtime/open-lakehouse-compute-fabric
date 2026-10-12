let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    s = Table.SelectColumns(Source, {"first","last"}),
    Result = Table.CombineColumns(s, {"first","last"}, Combiner.CombineTextByDelimiter(" "), "full")
in
    Result
