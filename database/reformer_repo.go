package database

type reformerRepo struct{}

func NewReformerRepo() Repo {
	return reformerRepo{}
}

func (r reformerRepo) All() ([]Database, error) {
	return []Database{}, nil
}
