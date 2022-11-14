package queue

import (
	"database/sql"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
)

var instance *bun.DB

func GetDatabase() *bun.DB {
	if instance != nil {
		return instance
	}

	dsn := "postgres://maddox:@localhost:5432/maddox?sslmode=disable&application_name=JobQueue"
	// dsn := "unix://user:pass@dbname/var/run/postgresql/.s.PGSQL.5432"
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))

	maxOpenConns := 1
	sqldb.SetMaxOpenConns(maxOpenConns)
	sqldb.SetMaxIdleConns(maxOpenConns)

	instance = bun.NewDB(sqldb, pgdialect.New())
	return instance
}
