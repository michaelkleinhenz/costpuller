package main

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/costexplorer"
	"github.com/jinzhu/now"
)

// AWSPuller implements the AWS query client
type AWSPuller struct {
	debug bool
}

// NewAWSPuller returns a new AWS client.
func NewAWSPuller(debug bool) *AWSPuller {
	awsp := new(AWSPuller)
	awsp.debug = debug
	return awsp
}

// PullData retrieves a raw data set.
func (a *AWSPuller) PullData(accountID string, month string, costType string) (map[string]float64, error) {
	// check month format
	focusMonth, err := time.Parse("2006-01", month)
	if err != nil {
		log.Printf("[pullawsdata] month format error: %v\n", err)
		return nil, err
	}
	beginningOfMonth := now.With(focusMonth).BeginningOfMonth()
	endOfMonth := now.With(focusMonth).EndOfMonth().Add(time.Hour * 24)
	dayStart := beginningOfMonth.Format("2006-01-02")
	dayEnd := endOfMonth.Format("2006-01-02")
	log.Printf("[pullawsdata] using date range %s to %s", dayStart, dayEnd)
	// retrieve AWS cost
	session := session.Must(session.NewSessionWithOptions(session.Options{
    SharedConfigState: session.SharedConfigEnable,
	}))
	svc := costexplorer.New(session)
	granularity := "MONTHLY"
	metricsBlendedCost := costType
	log.Printf("[pullawsdata] using cost type %s", metricsBlendedCost)
	dimensionLinkedAccountKey := "LINKED_ACCOUNT"
	dimensionLinkedAccountValue := accountID
	groupByDimension := "DIMENSION"
	groupByServce := "SERVICE"
	costAndUsageService, err := svc.GetCostAndUsage(&costexplorer.GetCostAndUsageInput{
		TimePeriod: &costexplorer.DateInterval{
			Start: &dayStart,
			End: &dayEnd,
		},
		Granularity: &granularity,
		Metrics: []*string{&metricsBlendedCost},
		Filter: &costexplorer.Expression{
			Dimensions: &costexplorer.DimensionValues{
				Key: &dimensionLinkedAccountKey,
				Values: []*string{&dimensionLinkedAccountValue},
			},
		},
		GroupBy: []*costexplorer.GroupDefinition{
			&costexplorer.GroupDefinition{
				Type: &groupByDimension,
				Key: &groupByServce,
			},
		},
	})
	if err != nil {
		log.Printf("[pullawsdata] error retrieving aws service cost report: %v\n", err)
		return nil, err
	}
	if a.debug {
		log.Println("[pullawsdata] received service breakdown report:")
		log.Println(*costAndUsageService)
	}
	costAndUsageTotal, err := svc.GetCostAndUsage(&costexplorer.GetCostAndUsageInput{
		TimePeriod: &costexplorer.DateInterval{
			Start: &dayStart,
			End: &dayEnd,
		},
		Granularity: &granularity,
		Metrics: []*string{&metricsBlendedCost},
		Filter: &costexplorer.Expression{
			Dimensions: &costexplorer.DimensionValues{
				Key: &dimensionLinkedAccountKey,
				Values: []*string{&dimensionLinkedAccountValue},
			},
		},
	})
	if err != nil {
		log.Printf("[pullawsdata] error retrieving aws total cost report: %v\n", err)
		return nil, err
	}
	if a.debug {
		log.Println("[pullawsdata] received total report:")
		log.Println(*costAndUsageTotal)
	}
	// decode total value
	totalAWSStr := *(*(*costAndUsageTotal.ResultsByTime[0]).Total[metricsBlendedCost]).Amount
	totalAWS, err := strconv.ParseFloat(totalAWSStr, 64)
	if err != nil {
		log.Printf("[pullawsdata] error converting aws total value: %v", err)
		return nil, err
	}
	unitAWS := *(*(*costAndUsageTotal.ResultsByTime[0]).Total[metricsBlendedCost]).Unit
	if unitAWS != "USD" {
		log.Printf("[pullawsdata] pulled unit is not USD: %s", unitAWS)
		return nil, fmt.Errorf("pulled unit is not USD: %s", unitAWS)
	}
	// decode service data
	var totalService float64 = 0
	serviceResults := make(map[string]float64)
	resultsByTime := costAndUsageService.ResultsByTime
	if len(resultsByTime) != 1 {
		log.Printf("[pullawsdata] warning account %s does not have exactly one service results by time (has %d)", accountID, len(resultsByTime))
		return serviceResults, nil
	}
	serviceGroups := resultsByTime[0].Groups
	for _, group := range(serviceGroups) {
		if len(group.Keys) != 1 {
			log.Printf("[pullawsdata] warning account %s service group does not have exactly one key", accountID)
			return serviceResults, fmt.Errorf("[pullawsdata] warning account %s service group does not have exactly one key", accountID)
		}
		key := group.Keys[0]
		valueStr := group.Metrics[costType].Amount
		unit := group.Metrics[costType].Unit
		if *unit != unitAWS {
			log.Printf("[pullawsdata] error: inconsistent units (%s vs %s) for account %s", unitAWS, *unit, accountID)
			return nil, fmt.Errorf("[pullawsdata] error: inconsistent units (%s vs %s) for account %s", unitAWS, *unit, accountID)
		}
		value, err := strconv.ParseFloat(*valueStr, 64)
		if err != nil {
			log.Printf("[pullawsdata] error converting aws service value: %v", err)
			return nil, err
		}
		serviceResults[*key] = value
		totalService += value
	}
	if math.Round(totalService*100)/100 != math.Round(totalAWS*100)/100  {
		log.Printf("[pullawsdata] error: account %s service total %f does not match aws total %f", accountID, totalService, totalAWS)
		return nil, fmt.Errorf("[pullawsdata] error: account %s service total %f does not match aws total %f", accountID, totalService, totalAWS)
	}
	return serviceResults, nil
}

// NormalizeResponse normalizes a Response object data into report categories.
func (a *AWSPuller) NormalizeResponse(daterange string, accountID string, serviceResults map[string]float64) ([]string, error) {
	// format is: 
	// date, clusterId, accountId, PO, clusterType, usageType, product, infra, numberUsers, dataTransfer, machines, storage, keyMgmnt, registrar, dns, other, tax, refund
	output := make([]string, 18)
	for idx := range(output) {
		output[idx] = "PENDING"
	}
	// infra is always AWS
	output[7] = "AWS"
	// set date - we use the first service entry
	output[0] = daterange
	// set clusterID
	output[2] = accountID
	// init cost values
	output[9] = "0"
	output[10] = "0"
	output[11] = "0"
	output[12] = "0"
	output[13] = "0"
	output[14] = "0"
	output[15] = "0"
	output[17] = "0"	
	// nomalize cost values
	var ec2Val float64 = 0
	var kmVal float64 = 0
	var otherVal float64 = 0
	for key, value := range(serviceResults) {
		switch key {
		case "AWS Data Transfer":
			output[9] = fmt.Sprintf("%f", value)
		case "Amazon Elastic Compute Cloud - Compute":
			ec2Val += value
		case "EC2 - Other":
			ec2Val += value
		case "Amazon Simple Storage Service":
			output[11] = fmt.Sprintf("%f", value)
		case "AWS Key Management Service":
			kmVal += value
		case "AWS Secrets Manager":
			kmVal += value
		case "Amazon Route 53":
			output[14] = fmt.Sprintf("%f", value)
		case "Tax":
			output[16] = fmt.Sprintf("%f", value)
		default:
			otherVal += value
		}
	}
	// EC2
	output[10] = fmt.Sprintf("%f", ec2Val)
	// key management
	output[12] = fmt.Sprintf("%f", kmVal)
	// store other total
	output[15] = fmt.Sprintf("%f", otherVal)
	return output, nil
}

// CheckResponseConsistency checks the response consistency with various checks. Returns the calculated total.
func (a *AWSPuller) CheckResponseConsistency(account AccountEntry, results map[string]float64) (float64, error) {
	var total float64 = 0
	for _, value := range(results) {
		// add up value
		total += value
	}
	// check account meta deviation if standardvalue is given
	if account.Standardvalue > 0 {
		diff := account.Standardvalue - total
		diffAbs := math.Abs(diff)
		diffPercent := (diffAbs / account.Standardvalue) * 100
		if diffPercent > float64(account.Deviationpercent) {
			return total, fmt.Errorf("deviation check failed: deviation is %.2f (%.2f%%), max deviation allowed is %d%% (value was %.2f, standard value %.2f)", diffAbs, diffPercent, account.Deviationpercent, total, account.Standardvalue)
		}	
	}
	if a.debug {
		log.Println("[CheckResponseConsistency] service struct:")
		log.Println(results)
		log.Printf("[CheckResponseConsistency] total retrieved from service struct is %f", total)
	}
	return total, nil
}
