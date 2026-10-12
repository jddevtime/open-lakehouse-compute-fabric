let
 S = Sql.Database(ServerName, DbName),
 t = S{[Schema="dbo",Item="X"]}[Data]
in
 t
