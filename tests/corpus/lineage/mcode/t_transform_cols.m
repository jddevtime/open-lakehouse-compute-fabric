let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    s = Table.SelectColumns(Source, {"a","b"}),
    Result = Table.TransformColumns(s, {{"a", Text.Upper}})
in
    Result
