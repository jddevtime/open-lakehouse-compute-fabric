let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    s = Table.SelectColumns(Source, {"id"}),
    Result = Table.ExpandRecordColumn(s, "id", {"x"}, {"id.x"})
in
    Result
