let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    s = Table.SelectColumns(Source, {"full"}),
    Result = Table.SplitColumn(s, "full", Splitter.SplitTextByDelimiter(" "), {"f","l"})
in
    Result
