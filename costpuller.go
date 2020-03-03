package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"strings"

	"gopkg.in/yaml.v2"
)

// Response describes the toplevel data structure
type Response struct {
	Meta MetaSection `json:"meta"`
	Data []DataSection `json:"data"`
}

// MetaSection describes a child data structure
type MetaSection struct {
	Count float64 `json:"count"`
	Filter FilterSection `json:"filter"`
	Total TotalSection `json:"total"`
}

// FilterSection describes a child data structure
type FilterSection struct {
	Account []string `json:"account"`
}

// TotalSection describes a child data structure
type TotalSection struct {
	Cost CostSection `json:"cost"`
}

// CostSection describes a child data structure
type CostSection struct {
	Value float64 `json:"value"`
	Unit string `json:"units"`
}

// DataSection describes a child data structure
type DataSection struct {
	Date string `json:"date"`
	Services []ServiceSection `json:"services"`
}

// ServiceSection describes a child data structure
type ServiceSection struct {
	Service string `json:"service"`
	Values []ValueSection `json:"values"`
}

// ValueSection describes a child data structure
type ValueSection struct {
	Date string `json:"date"`
	Service string `json:"service"`
	Cost CostSection `json:"cost"`
}

func main() {
	log.Println("[main] costpuller starting..")
	// create data holder
	csvData := make([][]string, 0)
	// check if cookie param is present
	if len(os.Args) != 2 {
		log.Fatal("[main] no cookie parameter given!")
	}
	// retrieve cookie
	cookieStr := os.Args[1]
	log.Printf("[main] using cookie %s\n", cookieStr)
	cookieDeserialized := deserializeCurlCookie(cookieStr)
	// get account lists
	accounts, err := getAccountSets()
	if err != nil {
		log.Fatalf("[main] error unmarshalling accounts file: %v", err)
	}
	// open output file
	outfile, err := os.Create("output.csv")
	if err != nil {
		log.Fatalf("[main] error creating output file: %v", err)
	}
	defer outfile.Close()
	client := &http.Client{}
	// iterate over account lists
	for group, accountList := range(accounts) {
		csvData = appendCSVHeader(csvData, group)
		if err != nil {
			log.Fatalf("[main] error writing header to output file: %v", err)
		}
		for _, account := range(accountList) {
			log.Printf("[main] pulling data for account %s (group %s)\n", account, group)			
			// TODO
			result, err := pullData(client, account, cookieDeserialized)
			if err != nil {
				log.Fatalf("[main] error pulling data from service: %v", err)
			}
			parsed, err := parseResponse(result)
			if err != nil {
				log.Fatalf("[main] error parsing data from service: %v", err)
			}
			err = checkResponseConsistency(parsed)
			if err != nil {
				log.Fatalf("[main] error checking consistency of response for account data %s: %v", account, err)
			}
			log.Printf("[main] successful consistency check for data on account %s\n", account)
			normalized, err := normalizeResponse(parsed)
			if err != nil {
				log.Fatalf("[main] error normalizing data from service: %v", err)
			}
			csvData = appendCSVData(csvData, account, normalized)
		}
	}
	// write data to csv
	err = writeCSV(outfile, csvData)
	if err != nil {
		log.Fatalf("[main] error writing to output file: %v", err)
	}
	// done
	log.Println("[main] operation done")
}

func deserializeCurlCookie(curlCookie string) map[string]string {
	deserialized := make(map[string]string)
	cookieElements := strings.Split(curlCookie, "; ")
	for _, cookieStr := range(cookieElements) {
		keyValue := strings.Split(cookieStr, "=")
		deserialized[keyValue[0]] = keyValue[1]
	}
	return deserialized
}

func parseResponse(response []byte) (*Response, error) {
	responseData := new(Response)
	err := json.Unmarshal(response, responseData)
	if err != nil {
		log.Printf("[parseresponse] error parsing json: %v\n", err)
		return nil, err
	}
	return responseData, nil
}

func normalizeResponse(response *Response) ([]string, error) {
	// format is: 
	// date, clusterId, accountId, PO, clusterType, usageType, product, infra, numberUsers, dataTransfer, machines, storage, keyMgmnt, registrar, dns, other, tax, refund
	// init fields with pending flag
	output := make([]string, 18)
	for idx := range(output) {
		output[idx] = "PENDING"
	}
	// infra is always AWS
	output[7] = "AWS"
	// set date - we use the first service entry
	output[0] = response.Data[0].Date
	// set clusterID
	output[2] = response.Meta.Filter.Account[0]
	// init cost values
	output[9] = "0"
	output[10] = "0"
	output[11] = "0"
	output[12] = "0"
	output[13] = "0"
	output[14] = "0"
	output[15] = "0"
	output[16] = "0"
	output[17] = "0"	
	// nomalize cost values
	var otherVal float64 = 0
	for _, service := range(response.Data[0].Services) {
		switch service.Service {
		case "AWSDataTransfer":
			output[9] = fmt.Sprintf("%f", service.Values[0].Cost.Value)
		case "AmazonEC2":
			output[10] = fmt.Sprintf("%f", service.Values[0].Cost.Value)
		case "AmazonS3":
			output[11] = fmt.Sprintf("%f", service.Values[0].Cost.Value)
		case "awskms":
			output[12] = fmt.Sprintf("%f", service.Values[0].Cost.Value)
		case "AmazonRoute53":
			output[14] = fmt.Sprintf("%f", service.Values[0].Cost.Value)
		default:
			otherVal += service.Values[0].Cost.Value
		}
	}
	// store other total
	output[15] = fmt.Sprintf("%f", otherVal)
	// return result
	return output, nil
}

func checkResponseConsistency(response *Response) error {
	// check that there is exactly one entry in toplevel data
	if len(response.Data) != 1 {
		return fmt.Errorf("response data has length of %d instead of 1", len(response.Data))
	}
	// check that there is at least one service entry
	if len(response.Data[0].Services) == 0 {
		return errors.New("services array is empty")
	}
	var foundDate string = response.Data[0].Date
	var foundUnit string = response.Meta.Total.Cost.Unit
	var total float64 = 0
	for _, service := range(response.Data[0].Services) {
		// check that there is exactly one value section in services
		if len(service.Values) != 1 {
			return fmt.Errorf("service %s has more than exactly one values section (length is %d)", service.Service, len(service.Values))
		}
		// check date consistency
		if foundDate != service.Values[0].Date {
			return fmt.Errorf("service %s date stamp differs (%s vs %s)", service.Service, service.Values[0].Date, foundDate)
		}
		// check unit consistency
		if foundUnit != service.Values[0].Cost.Unit {
			return fmt.Errorf("service %s unit differs (%s vs %s)", service.Service, service.Values[0].Cost.Unit, foundUnit)
		}
		// add up value
		total += service.Values[0].Cost.Value
	}
	// check totals of all services is same as total in meta
	if math.Round(total*100)/100 != math.Round(response.Meta.Total.Cost.Value*100)/100 {
		return fmt.Errorf("total cost differs from meta and total of services (%f vs %f)", response.Meta.Total.Cost.Value, total)
	}
	return nil
}

func pullData(client *http.Client, accountID string, cookieMap map[string]string) ([]byte, error) {
	// create request
	req, err := http.NewRequest("GET", "https://cloud.redhat.com/api/cost-management/v1/reports/aws/costs/", nil)
	if err != nil {
		log.Printf("[pulldata] error creating request: %v ", err)
		return nil, err
	}
	// add get params
	q := req.URL.Query()
	q.Add("filter[time_scope_units]", "month")
	q.Add("filter[time_scope_value]", "-2")
	q.Add("filter[resolution]", "monthly")
	q.Add("filter[account]", accountID)
	q.Add("group_by[service]", "*")
	req.URL.RawQuery = q.Encode()
	// add cookies
	for cookieKey, cookieValue := range(cookieMap) {
		thisCookie := new(http.Cookie)
		thisCookie.Name = cookieKey
		thisCookie.Value = cookieValue
		req.AddCookie(thisCookie)
	}
	// set headers
	req.Header.Set("authority", "cloud.redhat.com")
	req.Header.Set("pragma", "no-cache")
	req.Header.Set("cache-control", "no-cache")
	req.Header.Set("accept", "application/json, text/plain, */*")
	req.Header.Set("referer", "https://cloud.redhat.com/beta/cost-management/")
	// execute request
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[pulldata] error pulling data from service: %v ", err)
		return nil, err
	}
	// check response
	if resp.StatusCode != 200 {
		log.Println("[pulldata] error pulling data from server")
		return nil, fmt.Errorf("error fetching data from service, returned status %d", resp.StatusCode)
	}
	// read body
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[pulldata] error reading body data: %v ", err)
		return nil, err
	}
	// return result
	return bodyBytes, nil
}

func appendCSVHeader(csvData [][]string, group string) [][]string {
	log.Printf("[appendcsvheader] appended header for group %s\n", group)
	header := make([]string, 1)
	header[0] = group
	return append(csvData, header)
}

func appendCSVData(csvData [][]string, account string, data []string) [][]string {
	log.Printf("[appendcsvdata] appended data for account %s\n", account)
	return append(csvData, data)
}

func writeCSV(outfile *os.File, data [][]string) error {
	writer := csv.NewWriter(outfile)
	defer writer.Flush()
	for _, value := range data {
		err := writer.Write(value)
		if err != nil {
			log.Printf("[writecsv] error writing csv data to file: %v ", err)
			return err
		}
	}
	return nil
}

func getAccountSets() (map[string][]string, error) {
	accounts := make(map[string][]string)
	yamlFile, err := ioutil.ReadFile("accounts.yaml")
	if err != nil {
			log.Printf("[getaccountsets] error reading accounts file: %v ", err)
			return nil, err
	}
	err = yaml.Unmarshal(yamlFile, accounts)
	if err != nil {
			log.Fatalf("[getaccountsets] error unmarshalling accounts file: %v", err)
			return nil, err
	}
	return accounts, nil
}