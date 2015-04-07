package database

import (
	"database/sql"
	"fmt"

	"github.com/pivotal-golang/lager"
)

const reformersQueryPattern = `
SELECT tables.table_schema AS db
FROM   information_schema.tables AS tables
JOIN   ( SELECT DISTINCT dbs.Db AS Db from mysql.db AS dbs WHERE (dbs.Insert_priv = 'N' OR dbs.Update_priv = 'N' OR dbs.Create_priv = 'N') ) AS dbs ON tables.table_schema = dbs.Db
JOIN   %s.service_instances AS instances ON tables.table_schema = instances.db_name COLLATE utf8_general_ci
GROUP  BY tables.table_schema
HAVING ROUND(SUM(tables.data_length + tables.index_length) / 1024 / 1024, 1) < MAX(instances.max_storage_mb)
`

func NewReformerRepo(brokerDBName string, db *sql.DB, logger lager.Logger) Repo {
	query := fmt.Sprintf(reformersQueryPattern, brokerDBName)
	return newRepo(query, db, logger, "quota reformer")
}
