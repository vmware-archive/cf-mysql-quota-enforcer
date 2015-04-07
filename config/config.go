package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
)

type Config struct {
	Host         string
	Port         int
	User         string
	Password     string
	BrokerDBName string
}

func Load(filePath string) (Config, error) {
	var config Config
	filePath, err := filepath.Abs(filePath)
	if err != nil {
		return config, errors.New(fmt.Sprintf("Making config file path absolute: %s", err.Error()))
	}

	configBytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return config, errors.New(fmt.Sprintf("Reading config file: %s", err.Error()))
	}

	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		return config, errors.New(fmt.Sprintf("Unmarshalling config file: %s", err.Error()))
	}

	return config, nil
}
