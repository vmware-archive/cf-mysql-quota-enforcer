package database

import (
	"database/sql"
	"fmt"

	"github.com/pivotal-golang/lager"
)

const violatorsQueryPattern = `
SELECT violators.name AS violator_db, violators.user AS violator_user
FROM (
	SELECT dbs.name, dbs.user, tables.data_length, tables.index_length
	FROM   (
		SELECT DISTINCT Db AS name, User AS user from mysql.db
		WHERE  (Insert_priv = 'Y' OR Update_priv = 'Y' OR Create_priv = 'Y') AND User <> '%s'
	) AS dbs
	JOIN %s.service_instances AS instances ON dbs.name = instances.db_name COLLATE utf8_general_ci
	JOIN information_schema.tables AS tables ON tables.table_schema = dbs.name
	GROUP BY dbs.name
	HAVING ROUND(SUM(COALESCE(tables.data_length + tables.index_length,0) / 1024 / 1024), 1) >= MAX(instances.max_storage_mb)
) AS violators
`

func NewViolatorRepo(brokerDBName, adminUser string, db *sql.DB, logger lager.Logger) Repo {
	query := fmt.Sprintf(violatorsQueryPattern, adminUser, brokerDBName)
	return newRepo(query, db, logger, "quota violator")
}
