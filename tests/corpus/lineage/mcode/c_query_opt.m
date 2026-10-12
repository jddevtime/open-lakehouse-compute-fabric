let
 S = Sql.Database("srv","erp",[Query="SELECT a, b FROM dbo.t1 JOIN dbo.t2 ON t1.id=t2.id"])
in
 S
