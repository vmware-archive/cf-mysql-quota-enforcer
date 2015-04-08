package config_test

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/fraenkel/candiedyaml"
	. "github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/config"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Config", func() {

	var (
		configToWrite  map[string]string
		configFilePath string
		tempDir        string
	)

	BeforeEach(func() {
		configToWrite = map[string]string{
			"Host":     "fake-host",
			"Port":     "9999",
			"User":     "fake-user",
			"Password": "fake-password",
			"DBName":   "fake-db-name",
		}
	})

	JustBeforeEach(func() {
		var err error
		tempDir, err = ioutil.TempDir(os.TempDir(), "quota-enforcer-config")
		Expect(err).NotTo(HaveOccurred())
		configFilePath = filepath.Join(tempDir, "config.yml")

		writeConfig(configFilePath, configToWrite)
	})

	AfterEach(func() {
		err := os.RemoveAll(tempDir)
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("Load", func() {

		It("returns an error when config file does not exist", func() {
			config, err := Load("/fake-path/fake-config.yml")
			Expect(config).To(BeNil())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("/fake-path/fake-config.yml"))
		})

		It("loads a valid config file", func() {
			config, err := Load(configFilePath)
			Expect(err).ToNot(HaveOccurred())

			Expect(*config).To(Equal(Config{
				Host:     "fake-host",
				Port:     9999,
				User:     "fake-user",
				Password: "fake-password",
				DBName:   "fake-db-name",
			}))
		})

		Context("when Host is not specified", func() {
			BeforeEach(func() {
				delete(configToWrite, "Host")
			})

			It("returns a validation error", func() {
				config, err := Load(configFilePath)
				Expect(err).To(HaveOccurred())
				Expect(config).To(BeNil())
				Expect(err.Error()).To(ContainSubstring("Host"))
			})
		})

		Context("when Port is not specified", func() {
			BeforeEach(func() {
				delete(configToWrite, "Port")
			})

			It("returns a validation error", func() {
				config, err := Load(configFilePath)
				Expect(err).To(HaveOccurred())
				Expect(config).To(BeNil())
				Expect(err.Error()).To(ContainSubstring("Port"))
			})
		})

		Context("when User is not specified", func() {
			BeforeEach(func() {
				delete(configToWrite, "User")
			})

			It("returns a validation error", func() {
				config, err := Load(configFilePath)
				Expect(err).To(HaveOccurred())
				Expect(config).To(BeNil())
				Expect(err.Error()).To(ContainSubstring("User"))
			})
		})

		Context("when Password is not specified", func() {
			BeforeEach(func() {
				delete(configToWrite, "Password")
			})

			It("allows blank passwords", func() {
				config, err := Load(configFilePath)
				Expect(err).ToNot(HaveOccurred())

				Expect(config.Password).To(BeEmpty())
			})
		})

		Context("when DBName is not specified", func() {
			BeforeEach(func() {
				delete(configToWrite, "DBName")
			})

			It("allows blank DBName", func() {
				config, err := Load(configFilePath)
				Expect(err).ToNot(HaveOccurred())

				Expect(config.DBName).To(BeEmpty())
			})
		})
	})
})

func writeConfig(configFilePath string, configToWrite map[string]string) {
	file, err := os.Create(configFilePath)
	Expect(err).ToNot(HaveOccurred())
	defer file.Close()

	encoder := candiedyaml.NewEncoder(file)
	err = encoder.Encode(configToWrite)
	Expect(err).ToNot(HaveOccurred())
}
