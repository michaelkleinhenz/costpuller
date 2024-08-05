package main

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/costexplorer"
	"github.com/aws/aws-sdk-go/service/organizations"
	"github.com/jinzhu/now"
	"google.golang.org/api/sheets/v4"
)

const AwsTagCostpullerCategory = "costpuller_category"

const AwsMetadataDescription = "description"
const AwsMetadataStatus = "status"

// AwsPuller implements the AWS query client
type AwsPuller struct {
	session *session.Session
	debug   bool
}

// NewAwsPuller returns a new AWS client.
func NewAwsPuller(profile string, debug bool) *AwsPuller {
	awsp := new(AwsPuller)
	awsp.session = session.Must(session.NewSessionWithOptions(session.Options{
		Profile:           profile,
		SharedConfigState: session.SharedConfigEnable,
	}))
	awsp.debug = debug
	return awsp
}

// PullData retrieves a raw data set.
func (a *AwsPuller) PullData(accountID string, month string, costType string) (map[string]float64, error) {
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
	// retrieve AWS cost
	svc := costexplorer.New(a.session)
	granularity := "MONTHLY"
	dimensionLinkedAccountKey := "LINKED_ACCOUNT"
	dimensionLinkedAccountValue := accountID
	groupByDimension := "DIMENSION"
	groupByService := "SERVICE"
	costAndUsageService, err := svc.GetCostAndUsage(&costexplorer.GetCostAndUsageInput{
		TimePeriod: &costexplorer.DateInterval{
			Start: &dayStart,
			End:   &dayEnd,
		},
		Granularity: &granularity,
		Metrics:     []*string{&costType},
		Filter: &costexplorer.Expression{
			Dimensions: &costexplorer.DimensionValues{
				Key:    &dimensionLinkedAccountKey,
				Values: []*string{&dimensionLinkedAccountValue},
			},
		},
		GroupBy: []*costexplorer.GroupDefinition{
			{
				Type: &groupByDimension,
				Key:  &groupByService,
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
			End:   &dayEnd,
		},
		Granularity: &granularity,
		Metrics:     []*string{&costType},
		Filter: &costexplorer.Expression{
			Dimensions: &costexplorer.DimensionValues{
				Key:    &dimensionLinkedAccountKey,
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
	totalAWSStr := *costAndUsageTotal.ResultsByTime[0].Total[costType].Amount
	totalAWS, err := strconv.ParseFloat(totalAWSStr, 64)
	if err != nil {
		log.Printf("[pullawsdata] error converting aws total value: %v", err)
		return nil, err
	}
	unitAWS := *costAndUsageTotal.ResultsByTime[0].Total[costType].Unit
	if unitAWS != "USD" {
		log.Printf("[pullawsdata] pulled unit is not USD: %s", unitAWS)
		return nil, fmt.Errorf("pulled unit is not USD: %s", unitAWS)
	}
	// decode service data
	var totalService float64 = 0
	serviceResults := make(map[string]float64)
	resultsByTime := costAndUsageService.ResultsByTime
	if len(resultsByTime) != 1 {
		log.Printf(
			"[pullawsdata] warning account %s does not have exactly one service results by time (has %d)",
			accountID,
			len(resultsByTime),
		)
		return serviceResults, nil
	}
	serviceGroups := resultsByTime[0].Groups
	for _, group := range serviceGroups {
		if len(group.Keys) != 1 {
			err := fmt.Errorf(
				"[pullawsdata] warning account %s service group does not have exactly one key",
				accountID,
			)
			log.Printf(err.Error())
			return serviceResults, err
		}
		key := group.Keys[0]
		valueStr := group.Metrics[costType].Amount
		unit := group.Metrics[costType].Unit
		if *unit != unitAWS {
			err := fmt.Errorf(
				"[pullawsdata] error: inconsistent units (%s vs %s) for account %s",
				unitAWS,
				*unit,
				accountID,
			)
			log.Printf(err.Error())
			return nil, err
		}
		value, err := strconv.ParseFloat(*valueStr, 64)
		if err != nil {
			log.Printf("[pullawsdata] error converting aws service value: %v", err)
			return nil, err
		}
		serviceResults[*key] = value
		totalService += value
	}
	if math.Round(totalService*100)/100 != math.Round(totalAWS*100)/100 {
		err := fmt.Errorf(
			"[pullawsdata] error: account %s service total %f does not match aws total %f",
			accountID,
			totalService,
			totalAWS,
		)
		log.Printf(err.Error())
		return nil, err
	}
	return serviceResults, nil
}

// NormalizeResponse normalizes a Response object data into report categories.
func (a *AwsPuller) NormalizeResponse(
	group string,
	dateRange string,
	accountID string,
	serviceResults map[string]float64,
) (*sheets.RowData, error) {
	// Format is:
	//   [0-9]    group, date, clusterId, accountId, PO, clusterType, usageType, product, infra, numberUsers,
	//   [10-18]  dataTransfer, machines, storage, keyMgmnt, registrar, dns, other, tax, rebate
	// Select entries 0, 1, 3, 8, and 10-18; omit entries 2, 4, 5, 6, 7, and 9
	output := sheets.RowData{Values: make([]*sheets.CellData, 13)}
	// set group
	output.Values[0] = newStringCell(group)
	// set date - we use the first service entry
	output.Values[1] = newStringCell(dateRange)
	// skip clusterId; set the accountId
	output.Values[2] = newStringCell(accountID)
	// skip PO, clusterType, usageType, and product; infra is always AWS
	output.Values[3] = newStringCell("AWS")

	// skip numberUsers; pick out and set the values for dataTransfer, storage,
	// dns, and tax; sum the remaining values into categories for machines,
	// keyMgmnt, and "other".
	var ec2Val float64 = 0
	var kmVal float64 = 0
	var otherVal float64 = 0

	// set default values, in case they are omitted from the data
	output.Values[4] = newNumberCell(0.0)
	output.Values[6] = newNumberCell(0.0)
	output.Values[9] = newNumberCell(0.0)
	output.Values[11] = newNumberCell(0.0)

	for key, value := range serviceResults {
		switch key {
		case "AWS Data Transfer":
			output.Values[4] = newNumberCell(value)
		case "Amazon Elastic Compute Cloud - Compute":
			ec2Val += value
		case "EC2 - Other":
			ec2Val += value
		case "Amazon Simple Storage Service":
			output.Values[6] = newNumberCell(value)
		case "AWS Key Management Service":
			kmVal += value
		case "AWS Secrets Manager":
			kmVal += value
		case "Amazon Route 53":
			output.Values[9] = newNumberCell(value)
		case "Tax":
			output.Values[11] = newNumberCell(value)
		default:
			otherVal += value
		}
	}
	// EC2 ("machines")
	output.Values[5] = newNumberCell(ec2Val)
	// key management
	output.Values[7] = newNumberCell(kmVal)
	// registrar (always zero??)
	output.Values[8] = newNumberCell(0.0)
	// "other" total
	output.Values[10] = newNumberCell(otherVal)
	// rebate (always zero??)
	output.Values[12] = newNumberCell(0.0)
	return &output, nil
}

// CheckResponseConsistency checks the response consistency with various checks. Returns the calculated total.
func (a *AwsPuller) CheckResponseConsistency(account AccountEntry, results map[string]float64) (float64, error) {
	var total float64 = 0
	for _, value := range results {
		// add up value
		total += value
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
	if a.debug {
		log.Println("[CheckResponseConsistency] service struct:")
		log.Println(results)
		log.Printf("[CheckResponseConsistency] total retrieved from service struct is %f", total)
	}
	return total, nil
}

// GetAwsAccountMetadata returns a map with accountIDs as keys and metadata key-value pairs map as value.
func (a *AwsPuller) GetAwsAccountMetadata() (map[string]map[string]string, error) {
	// get account list and basic metadata
	accounts, err := a.getAllAWSAccountData()
	if err != nil {
		return nil, err
	}
	// augment tags
	log.Println("[GetAwsAccountMetadata] starting tags pull for accounts")
	idx := 0
	for accountID := range accounts {
		idx++
		log.Printf("[GetAwsAccountMetadata] pulling tags for account %s (%d of %d)", accountID, idx, len(accounts))

		tags, err := a.getTagsForAWSAccount(accountID)
		if err != nil {
			return nil, err
		}
		for tagKey, tagValue := range tags {
			accounts[accountID][tagKey] = tagValue
		}
	}
	return accounts, nil
}

func (a *AwsPuller) getTagsForAWSAccount(accountID string) (map[string]string, error) {
	result := map[string]string{}
	svo := organizations.New(a.session)
	output, err := svo.ListTagsForResource(&organizations.ListTagsForResourceInput{
		NextToken:  nil,
		ResourceId: &accountID,
	})
	if err != nil {
		log.Printf("[pullawsdata] error getting account tags: %v", err)
		return nil, err
	}
	for _, e := range output.Tags {
		result[*e.Key] = *e.Value
	}
	for output.NextToken != nil && *output.NextToken != "" {
		output, err = svo.ListTagsForResource(&organizations.ListTagsForResourceInput{
			ResourceId: &accountID,
			NextToken:  output.NextToken,
		})
		if err != nil {
			log.Printf("[pullawsdata] error getting account tags: %v", err)
			return nil, err
		}
		for _, e := range output.Tags {
			result[*e.Key] = *e.Value
		}
	}
	return result, nil
}

func (a *AwsPuller) pullAccountData(
	svo *organizations.Organizations,
	result *map[string]map[string]string,
	nextToken *string,
) (*string, error) {
	limit := int64(10)
	output, err := svo.ListAccounts(&organizations.ListAccountsInput{
		MaxResults: &limit,
		NextToken:  nextToken,
	})
	if err != nil {
		log.Printf("[pullawsdata] error getting account list: %v", err)
		return nil, err
	}
	for _, e := range output.Accounts {
		(*result)[*e.Id] = map[string]string{
			AwsMetadataDescription: *e.Name,
			AwsMetadataStatus:      *e.Status,
		}
	}
	return output.NextToken, nil
}

func (a *AwsPuller) getAllAWSAccountData() (map[string]map[string]string, error) {
	result := map[string]map[string]string{}
	svo := organizations.New(a.session)
	log.Println("[pullawsdata] pulling all accounts metadata")
	nextToken, err := a.pullAccountData(svo, &result, nil)
	if err != nil {
		return nil, err
	}
	for nextToken != nil && *nextToken != "" {
		log.Printf("[pullawsdata] pulling more accounts metadata, pulled %d accounts", len(result))
		nextToken, err = a.pullAccountData(svo, &result, nextToken)
		if err != nil {
			log.Printf("[pullawsdata] error getting account list: %v", err)
			return nil, err
		}
	}
	log.Printf("[pullawsdata] done pulling accounts metadata, total pulled accounts: %d", len(result))
	return result, nil
}

func (a *AwsPuller) WriteAwsTags(accounts map[string][]AccountEntry) error {
	svo := organizations.New(a.session)
	categoryTag := AwsTagCostpullerCategory
	for category, accountEntries := range accounts {
		for _, accountEntry := range accountEntries {
			fmt.Printf("setting tag %s == %s for account %s...", categoryTag, category, accountEntry.AccountID)
			if !a.debug {
				_, err := svo.TagResource(&organizations.TagResourceInput{
					ResourceId: &accountEntry.AccountID,
					Tags: []*organizations.Tag{
						{Key: &categoryTag, Value: &category},
					},
				})
				if err != nil {
					return err
				}
				fmt.Println("done.")
			} else {
				fmt.Println("not done (debug mode).")
			}
		}
	}
	return nil
}

func newStringCell(val string) *sheets.CellData {
	return &sheets.CellData{UserEnteredValue: &sheets.ExtendedValue{StringValue: &val}}
}

func newNumberCell(val float64) *sheets.CellData {
	return &sheets.CellData{UserEnteredValue: &sheets.ExtendedValue{NumberValue: &val}}
}
