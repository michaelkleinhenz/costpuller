package main

import (
	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"log"
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
			err = checkResponse(result)
			if err != nil {
				log.Fatalf("[main] error checking plausability of response for account data %s: %v", account, err)
			}
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
	// format is: date, clusterId, accountId, PO, clusterType, usageType, product, infra, numberUsers, dataTransfer, machines, storage, keyMgmnt, registrar, dns, other, tax, refund
	// init fields with pending flag
	output := make([]string, 18)
	for idx := range(output) {
		output[idx] = "PENDING"
	}
	// set clusterID
	output[1] = response.Meta.Filter.Account[0]
}

func checkResponse(response *Response) error {
	// TODO do plausability checks
	return nil
}

/*
func parseResponse(response []byte) (map[string]string, error) {
	var parsed interface{}
	err := json.Unmarshal(response, &parsed)
	if err != nil {
		log.Printf("[parseresponse] error parsing json: %v\n", err)
		return nil, err
	}
	result := make(map[string]string)
	m := parsed.(map[string]interface{})
	for key, value := range(m) {
		switch key {
		case "meta":
			meta, ok := value.(map[string]interface{})
			if !ok {
				log.Println("[parseresponse] error casting meta section.")
				return nil, err
			}
			result, err = parseMeta(meta, result)
			if err != nil {
				log.Printf("[parseresponse] error parsing meta section: %v\n", err)
				return nil, err
			}
		case "data":
			dataArr, ok := value.([]interface{})
			if len(dataArr) != 1 {
				log.Println("[parseresponse] data array is not exactly one element long.")
				return nil, errors.New("data array is not exactly one element long")
			}
			dm, ok := dataArr[0].(map[string]interface{})
			if !ok {
				log.Println("[parseresponse] error casting data section.")
				return nil, err
			}		
			if _, ok := result["date"]; ok {
				// date is already set, do a consistency check
				if result["date"] != dm["date"] {
					log.Printf("[parseresponse] inconsistent date in json response: %s and %s\n", result["date"], dm["date"])
					return nil, err			
				}
			} else {
				// date not set yet, store it
				result["date"], ok = dm["date"].(string)
				if !ok {
					log.Println("[parseresponse] error casting date value to string.")
					return nil, err
				}			
			}
			// parse services
			sArr, ok := dm["services"].([]interface{})
			if !ok {
				log.Println("[parseresponse] error casting service array value to array.")
				return nil, err
			}
			result, err = parseServices(sArr, result)
			if err != nil {
				log.Printf("[parseresponse] error parsing services section: %v\n", err)
				return nil, err
			}
		}
	}
	return result, nil
}

func parseMeta(meta map[string]interface{}, data map[string]string) (map[string]string, error) {
	if count, ok := meta["count"]; ok {
		countVal, ok := count.(float64)
		if !ok {
			log.Println("[parsemeta] error casting count to float.")
			return nil, errors.New("error casting count to float")
		}
		if countVal != 1 {
			log.Println("[parsemeta] count is not exactly 1.")
			return nil, errors.New("count is not exactly 1")
		}
	} else {
		log.Println("[parsemeta] count not found in meta section.")
		return nil, errors.New("count not found in meta section")
	}
	if filter, ok := meta["filter"]; ok {
		filterTyped, ok := filter.(map[string]interface{})
		if !ok {
			log.Println("[parsemeta] error casting filter to map.")
			return nil, errors.New("error casting filter to map")	
		}

	} else {
		log.Println("[parsemeta] filter not found in meta section.")
		return nil, errors.New("filter not found in meta section")
	}
	if total, ok := meta["total"]; ok {
		totalTyped, ok := total.(map[string]interface{})
		if !ok {
			log.Println("[parsemeta] error casting total to map.")
			return nil, errors.New("error casting total to map")	
		}
		
	} else {
		log.Println("[parsemeta] total not found in meta section.")
		return nil, errors.New("total not found in meta section")
	}
	return data, nil
}

func parseServices(services []interface{}, data map[string]string) (map[string]string, error) {
	for _, serviceRaw := range(services) {
		serviceTyped, ok := serviceRaw.(map[string]interface{})
		if !ok {
			log.Println("[parseservices] error casting raw service to map.")
			return nil, errors.New("error casting raw service to map")
		}
		var serviceName string
		var serviceValue string
		for key, value := range(serviceTyped) {
			switch key {
			case "service":
				serviceName, ok = value.(string)
				if !ok {
					log.Println("[parseservices] error casting service name to string.")
					return nil, errors.New("error casting service name to string")
				}
			case "values":
				costWrapperEnv, ok := value.([]interface{})
				if !ok {
					log.Println("[parseservices] error casting values to array.")
					return nil, errors.New("error casting values to array")
				}
				if len(costWrapperEnv) != 1 {
					log.Println("[parseservices] values is not exactly one element long.")
					return nil, errors.New("values is not exactly one element long")
				}
				costWrapper, ok := costWrapperEnv[0].(map[string]interface{})
				if !ok {
					log.Println("[parseservices] error casting values to map.")
					return nil, errors.New("error casting values to map")
				}
				if _, ok := costWrapper["cost"]; ok {
					cost, ok := costWrapper["cost"].(map[string]interface{})
					if !ok {
						log.Println("[parseservices] error casting cost to map.")
						return nil, errors.New("error casting cost to map")
					}
					serviceValueFloat, ok := cost["value"].(float64)
					serviceValue = fmt.Sprintf("%f", serviceValueFloat)
					if !ok {
						log.Printf("[parseservices] error casting cost value to string: %s\n", cost["value"])
						return nil, errors.New("error casting cost value to string")
					}
				} else {
					log.Printf("[parseservices] cost element not available for service %s\n", serviceName)
					return nil, errors.New("cost element not available for service")
				}
			}
		}
		data[serviceName] = serviceValue
	}
	return data, nil
}
*/

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
	log.Printf("[appendcsvdata] appended header for account %s\n", account)
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