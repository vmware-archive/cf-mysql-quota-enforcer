package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fraenkel/candiedyaml"
	"gopkg.in/validator.v2"
)

type Config struct {
	Host     string `validate:"nonzero"`
	Port     int    `validate:"nonzero"`
	User     string `validate:"nonzero"`
	Password string //blank Password is allowed
	DBName   string //blank DBName is allowed
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

	err = config.Validate()
	if err != nil {
		return nil, err
	}

	return config, nil
}

func (c Config) Validate() error {
	err := validator.Validate(c)
	var errString string
	if err != nil {
		errString = formatErrorString(err)
	}

	if len(errString) > 0 {
		return errors.New(fmt.Sprintf("Validation errors: %s\n", errString))
	}
	return nil
}

func formatErrorString(err error) string {
	errs := err.(validator.ErrorMap)
	var errsString string
	for fieldName, validationMessage := range errs {
		errsString += fmt.Sprintf("%s : %s\n", fieldName, validationMessage)
	}
	return errsString
}
