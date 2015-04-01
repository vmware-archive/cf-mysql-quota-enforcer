package database

type Repo interface {
	All() ([]Database, error)
}
