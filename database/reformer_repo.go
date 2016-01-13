package database

import (
	"database/sql"
	"fmt"

	"github.com/pivotal-golang/lager"
)

// LEFT JOIN is required so that dropping all tables will restore write access
const reformersQueryPattern = `
SELECT reformers.name AS reformer_db, reformers.user AS reformer_user
FROM (
	SELECT violator_dbs.name, violator_dbs.user, tables.data_length, tables.index_length
	FROM   (
		SELECT DISTINCT Db AS name, User AS user from mysql.db
		WHERE  (Insert_priv = 'N' OR Update_priv = 'N' OR Create_priv = 'N') AND User <> '%s'
	) AS violator_dbs
	JOIN        %s.service_instances AS instances ON violator_dbs.name = instances.db_name COLLATE utf8_general_ci
	LEFT JOIN   information_schema.tables AS tables ON tables.table_schema = violator_dbs.name
	GROUP  BY   violator_dbs.name
	HAVING ROUND(SUM(COALESCE(tables.data_length + tables.index_length,0) / 1024 / 1024), 1) < MAX(instances.max_storage_mb)
) AS reformers
`

func NewReformerRepo(brokerDBName, adminUser string, db *sql.DB, logger lager.Logger) Repo {
	query := fmt.Sprintf(reformersQueryPattern, adminUser, brokerDBName)
	return newRepo(query, db, logger, "quota reformer")
}
