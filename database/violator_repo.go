package database

type violatorRepo struct {}

func NewViolatorRepo() Repo {
    return violatorRepo{}
}

func (r violatorRepo) All() ([]Database, error) {
    return []Database{}, nil
}