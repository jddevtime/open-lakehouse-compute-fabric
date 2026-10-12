let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    s = Table.SelectColumns(Source, {"a"}),
    Result = Table.AddColumn(s, "c", each Record.Field(_, "a"))
in
    Result
