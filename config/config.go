package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fraenkel/candiedyaml"
)

type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

func Load(filePath string) (*Config, error) {
	filePath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Making config file path absolute: %s", err.Error()))
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	config := new(Config)

	decoder := candiedyaml.NewDecoder(file)
	err = decoder.Decode(config)
	if err != nil {
		return nil, err
	}

	return config, nil
}
