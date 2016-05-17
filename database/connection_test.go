package database_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"time"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database"
)

var _ = Describe("NewConnection", func() {

	const (
		dbName     = "fake_db_name"
		dbUser     = "root"
		dbPassword = "password"
		dbPort     = 3306
		dbHost     = "10.244.7.2"
	)

	BeforeEach(func() {
		sendRequest("http://10.244.7.2:9200/stop_mysql", "POST")
		fmt.Println("Sent stop request. Waiting for server to stop...")
		time.Sleep(10 * time.Second)
	})

	Context("When the server is stopped", func() {

		It("fails to deliver a connection", func() {
			_, err := database.NewConnection(dbUser, dbPassword, dbHost, dbPort, dbName)
			fmt.Println(err.Error())
			Expect(err).To(HaveOccurred())
		})

	})

	Context("When the server is running but DB does not exist", func() {

		BeforeEach(func() {
			sendRequest("http://10.244.7.2:9200/start_mysql_join", "POST")
			fmt.Println("Sent start request. Waiting for server to come up...")
			time.Sleep(60 * time.Second)
		})

		It("fails to deliver a connection", func() {
			_, err := database.NewConnection(dbUser, dbPassword, dbHost, dbPort, dbName)
			fmt.Println(err.Error())
			Expect(err).To(HaveOccurred())
		})

	})

	Context("When the server is running and DB exists", func() {

		BeforeEach(func() {
			sendRequest("http://10.244.7.2:9200/start_mysql_join", "POST")
			fmt.Println("Sent start request. Waiting for server to come up...")
			time.Sleep(30 * time.Second)
			fmt.Println("Server is up, creating DB...")
			cmdName := "/bin/bash"
			cmdArgs := []string{"/Users/pivotal/workspace/cf-mysql-release/src/github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database/create-db-script"}
			cmd := exec.Command(cmdName, cmdArgs...)
			_, err := cmd.Output()
			Expect(err).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			cmdName := "/bin/bash"
			cmdArgs := []string{"/Users/pivotal/workspace/cf-mysql-release/src/github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database/drop-db-script"}
			cmd := exec.Command(cmdName, cmdArgs...)
			_, err := cmd.Output()
			Expect(err).ToNot(HaveOccurred())
		})

		It("successfuly delivers a connection", func() {
			_, err := database.NewConnection(dbUser, dbPassword, dbHost, dbPort, dbName)
			Expect(err).ToNot(HaveOccurred())
		})

	})

	Context("When the server takes time to start but that time is less than the configured timeout", func() {

		BeforeEach(func() {
			go func() {
				fmt.Println("I'm going to wait 30s...")
				time.Sleep(30 * time.Second)
				sendRequest("http://10.244.7.2:9200/start_mysql_join", "POST")
				fmt.Println("Sent start request. Waiting for server to come up...")
				time.Sleep(30 * time.Second)
				fmt.Println("Server is up, creating DB...")
				cmdName := "/bin/bash"
				cmdArgs := []string{"/Users/pivotal/workspace/cf-mysql-release/src/github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database/create-db-script"}
				cmd := exec.Command(cmdName, cmdArgs...)
				_, err := cmd.Output()
				Expect(err).ToNot(HaveOccurred())
			}()
		})

		AfterEach(func() {
			cmdName := "/bin/bash"
			cmdArgs := []string{"/Users/pivotal/workspace/cf-mysql-release/src/github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database/drop-db-script"}
			cmd := exec.Command(cmdName, cmdArgs...)
			_, err := cmd.Output()
			Expect(err).ToNot(HaveOccurred())
		})

		It("successfully delivers a connection", func() {
			_, err := database.NewConnection(dbUser, dbPassword, dbHost, dbPort, dbName)
			Expect(err).ToNot(HaveOccurred())
		})

	})

	FContext("When the server takes time to start but that time is more than the configured timeout", func() {

		BeforeEach(func() {
			go func() {
				fmt.Println("I'm going to wait over one min...")
				time.Sleep(70 * time.Second)
				sendRequest("http://10.244.7.2:9200/start_mysql_join", "POST")
				fmt.Println("Sent start request. Waiting for server to come up...")
				time.Sleep(30 * time.Second)
				fmt.Println("Server is up, creating DB...")
				cmdName := "/bin/bash"
				cmdArgs := []string{"/Users/pivotal/workspace/cf-mysql-release/src/github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database/create-db-script"}
				cmd := exec.Command(cmdName, cmdArgs...)
				cmd.Output()
			}()
		})

		AfterEach(func() {
			cmdName := "/bin/bash"
			cmdArgs := []string{"/Users/pivotal/workspace/cf-mysql-release/src/github.com/pivotal-cf-experimental/cf-mysql-quota-enforcer/database/drop-db-script"}
			cmd := exec.Command(cmdName, cmdArgs...)
			cmd.Output()
		})

		It("returns an error", func() {
			_, err := database.NewConnection(dbUser, dbPassword, dbHost, dbPort, dbName)
			Expect(err).To(HaveOccurred())
		})

	})

})

func sendRequest(endpoint string, method string) (string, error) {
	req, err := http.NewRequest(method, endpoint, nil)
	if err != nil {
		return "", err
	}
	req.SetBasicAuth("username", "password")

	resp, err := http.DefaultClient.Do(req)
	responseBody := ""
	if err != nil {
		return responseBody, fmt.Errorf("Failed to %s: %s", endpoint, err.Error())
	}

	if resp.Body != nil {
		responseBytes, _ := ioutil.ReadAll(resp.Body)
		responseBody = string(responseBytes)
	}

	if resp.StatusCode != http.StatusOK {
		return responseBody, fmt.Errorf("Non 200 response from %s: %s", endpoint, responseBody)
	}

	fmt.Sprintf("Successfully sent %s request to URL", endpoint)

	return responseBody, nil
}
