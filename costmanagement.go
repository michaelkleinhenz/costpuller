package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
)

// Response describes the toplevel data structure
type Response struct {
	Meta MetaSection   `json:"meta"`
	Data []DataSection `json:"data"`
}

// MetaSection describes a child data structure
type MetaSection struct {
	Count  float64       `json:"count"`
	Filter FilterSection `json:"filter"`
	Total  TotalSection  `json:"total"`
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
	TotalCost TotalCostSection `json:"total"`
}

// TotalCostSection describes a child data structure
type TotalCostSection struct {
	Value float64 `json:"value"`
	Unit  string  `json:"units"`
}

// DataSection describes a child data structure
type DataSection struct {
	Date     string           `json:"date"`
	Services []ServiceSection `json:"services"`
}

// ServiceSection describes a child data structure
type ServiceSection struct {
	Service string         `json:"service"`
	Values  []ValueSection `json:"values"`
}

// ValueSection describes a child data structure
type ValueSection struct {
	Date    string      `json:"date"`
	Service string      `json:"service"`
	Cost    CostSection `json:"cost"`
}

// CmPuller implements the Cost Management query client.
type CmPuller struct {
	debug      bool
	httpClient *http.Client
	cookieMap  map[string]string
}

// NewCmPuller returns a new Cost Management client.
func NewCmPuller(cookieMap map[string]string, debug bool) *CmPuller {
	cmp := new(CmPuller)
	cmp.debug = debug
	cmp.httpClient = &http.Client{}
	cmp.cookieMap = cookieMap
	return cmp
}

// PullData retrieves a raw data set.
func (c *CmPuller) PullData(accountID string) ([]byte, error) {
	// create request
	req, err := http.NewRequest(
		"GET",
		"https://cloud.redhat.com/api/cost-management/v1/reports/aws/costs/",
		nil,
	)
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
	for cookieKey, cookieValue := range c.cookieMap {
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
	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf("[pulldata] error pulling data from service: %v ", err)
		return nil, err
	}
	// check response
	if resp.StatusCode != 200 {
		log.Println("[pulldata] error pulling data from server")
		bodyBytes, _ := io.ReadAll(resp.Body)
		bodyStr := string(bodyBytes)
		return nil, fmt.Errorf(
			"error fetching data from service, returned status %d, url was %s\nBody: %s",
			resp.StatusCode,
			req.URL.String(),
			bodyStr,
		)
	}
	// read body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[pulldata] error reading body data: %v ", err)
		return nil, err
	}
	// return result
	return bodyBytes, nil
}

// ParseResponse parses a raw response into a Response object.
func (c *CmPuller) ParseResponse(response []byte) (*Response, error) {
	responseData := new(Response)
	err := json.Unmarshal(response, responseData)
	if err != nil {
		log.Printf("[parseresponse] error parsing json: %v\n", err)
		return nil, err
	}
	return responseData, nil
}

// NormalizeResponse normalizes a Response object data into report categories.
func (c *CmPuller) NormalizeResponse(response *Response) ([]string, error) {
	// format is:
	//   date, clusterId, accountId, PO, clusterType, usageType, product, infra, numberUsers,
	//   dataTransfer, machines, storage, keyMgmnt, registrar, dns, other, tax, refund
	// init fields with pending flag
	output := make([]string, 18)
	for idx := range output {
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
	// normalize cost values
	var otherVal float64 = 0
	for _, service := range response.Data[0].Services {
		switch service.Service {
		case "AWSDataTransfer":
			output[9] = fmt.Sprintf("%f", service.Values[0].Cost.TotalCost.Value)
		case "AmazonEC2":
			output[10] = fmt.Sprintf("%f", service.Values[0].Cost.TotalCost.Value)
		case "AmazonS3":
			output[11] = fmt.Sprintf("%f", service.Values[0].Cost.TotalCost.Value)
		case "awskms":
			output[12] = fmt.Sprintf("%f", service.Values[0].Cost.TotalCost.Value)
		case "AmazonRoute53":
			output[14] = fmt.Sprintf("%f", service.Values[0].Cost.TotalCost.Value)
		default:
			otherVal += service.Values[0].Cost.TotalCost.Value
		}
	}
	// store other total
	output[15] = fmt.Sprintf("%f", otherVal)
	// return result
	return output, nil
}

// CheckResponseConsistency checks the response consistency with various checks. Returns the calculated total.
func (c *CmPuller) CheckResponseConsistency(account AccountEntry, response *Response) (float64, error) {
	// TODO check base value consistence by comparing to a rough value given in the config
	// check that there is exactly one entry in toplevel data
	if len(response.Data) != 1 {
		return 0, fmt.Errorf("response data has length of %d instead of 1", len(response.Data))
	}
	// check that there is at least one service entry
	if len(response.Data[0].Services) == 0 {
		return 0, errors.New("services array is empty")
	}
	foundDate := response.Data[0].Date
	foundUnit := response.Meta.Total.Cost.TotalCost.Unit
	var total float64 = 0
	for _, service := range response.Data[0].Services {
		// check that there is exactly one value section in services
		if len(service.Values) != 1 {
			return 0, fmt.Errorf(
				"service %s has more than exactly one values section (length is %d)",
				service.Service,
				len(service.Values),
			)
		}
		// check date consistency
		if foundDate != service.Values[0].Date {
			return 0, fmt.Errorf(
				"service %s date stamp differs (%s vs %s)",
				service.Service,
				service.Values[0].Date,
				foundDate,
			)
		}
		// check unit consistency
		if foundUnit != service.Values[0].Cost.TotalCost.Unit {
			return 0, fmt.Errorf(
				"service %s unit differs (%s vs %s)",
				service.Service,
				service.Values[0].Cost.TotalCost.Unit,
				foundUnit,
			)
		}
		// add up value
		total += service.Values[0].Cost.TotalCost.Value
	}
	// check totals of all services is same as total in meta
	if math.Round(total*100)/100 != math.Round(response.Meta.Total.Cost.TotalCost.Value*100)/100 {
		return 0, fmt.Errorf(
			"total cost differs from meta and total of services (%f vs %f)",
			response.Meta.Total.Cost.TotalCost.Value,
			total,
		)
	}
	// check account meta deviation if standard value is given
	if account.Standardvalue > 0 {
		diff := account.Standardvalue - total
		diffAbs := math.Abs(diff)
		diffPercent := (diffAbs / account.Standardvalue) * 100
		if diffPercent > float64(account.Deviationpercent) {
			return total, fmt.Errorf(
				"deviation check failed: deviation is %.2f (%.2f%%), max deviation allowed is %d%% (value was %.2f, standard value %.2f)",
				diffAbs,
				diffPercent,
				account.Deviationpercent,
				total,
				account.Standardvalue,
			)
		}
	}
	return total, nil
}
