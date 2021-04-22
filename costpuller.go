package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"os/user"
	"sort"
	"strings"
	"time"

	"github.com/zellyn/kooky"
	"gopkg.in/yaml.v2"
)

// AccountEntry describes an account with metadata.
type AccountEntry struct {
	AccountID string `yaml:"accountid"`
	Standardvalue float64	`yaml:"standardvalue"`
	Deviationpercent int  `yaml:"deviationpercent"`
	Category string `yaml:"category"`
	Description string `yaml:"description"`
}

func main() {
	var err error
	log.Println("[main] costpuller starting..")
	// bootstrap
	usr, _ := user.Current()
	nowStr := time.Now().Format("20060102150405")
	// configure flags
	modePtr := flag.String("mode", "aws", "run mode, needs to be one of aws, cm or crosscheck")
	debugPtr := flag.Bool("debug", false, "outputs debug info")
	awsWriteTagsPtr := flag.Bool("awswritetags", false, "write tags to AWS accounts (USE WITH CARE!)")
	accountsFilePtr := flag.String("accounts", "accounts.yaml", "file to read accounts list from")
	taggedAccountsPtr := flag.Bool("taggedaccounts", false, "use the AWS tags as account list source")
	monthPtr := flag.String("month", "", "context month in format yyyy-mm, only for aws or crosscheck modes")
	costTypePtr := flag.String("costtype", "UnblendedCost", "cost type to pull, only for aws or crosscheck modes, one of AmortizedCost, BlendedCost, NetAmortizedCost, NetUnblendedCost, NormalizedUsageAmount, UnblendedCost, and UsageQuantity")
	cookiePtr := flag.String("cookie", "", "access cookie for cost management system in curl serialized format, only for cm or crosscheck modes")
	readcookiePtr := flag.Bool("readcookie", true, "reads the cookie from the Chrome cookies database, only for cm or crosscheck modes")
	cookieDbPtr := flag.String("cookiedb", fmt.Sprintf("%s/.config/google-chrome/Default/Cookies", usr.HomeDir), "path to Chrome cookies database file, only for cm or crosscheck modes")
	csvfilePtr := flag.String("csv", fmt.Sprintf("output-%s.csv", nowStr), "output file for csv data")
	reportfilePtr := flag.String("report", fmt.Sprintf("report-%s.txt", nowStr), "output file for data consistency report")
	flag.Parse()
	// create aws puller instance
	awsPuller := NewAWSPuller(*debugPtr)
	if *awsWriteTagsPtr {
		// we pull accounts from file
		accounts, err := getAccountSetsFromFile(*accountsFilePtr)
		if err != nil {
			log.Fatalf("[main] error getting accounts list: %v", err)
		}
		awsPuller.WriteAWSTags(accounts)
		os.Exit(0)
	}
	// open output files
	log.Printf("[main] using csv output file %s\n", *csvfilePtr)
	log.Printf("[main] using report output file %s\n", *reportfilePtr)
	// create data holder
	csvData := make([][]string, 0)
	// get account lists
	var accounts map[string][]AccountEntry
	if *taggedAccountsPtr {
		accounts, err = getAccountSetsFromAWS(awsPuller)
	} else {
		// we pull accounts from file
		accounts, err = getAccountSetsFromFile(*accountsFilePtr)
	}
	if err != nil {
		log.Fatalf("[main] error getting accounts list: %v", err)
	}
	sortedAccountKeys := sortedKeys(accounts)
	if err != nil {
		log.Fatalf("[main] error unmarshalling accounts file: %v", err)
	}
	// open csv output file
	outfile, err := os.Create(*csvfilePtr)
	if err != nil {
		log.Fatalf("[main] error creating output file: %v", err)
	}
	defer outfile.Close()
	// open report file
	reportfile, err := os.Create(*reportfilePtr)
	if err != nil {
		log.Fatalf("[main] error creating report file: %v", err)
	}
	defer reportfile.Close()
	// check for run mode
	switch *modePtr {
	case "aws":
		log.Println("[main] note: using credentials and account from env AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for aws pull")
		if *monthPtr == "" || *costTypePtr == "" {
			log.Fatal("[main] aws mode requested, but no month and/or costtype given (use --month=yyyy-mm, --costtype=type)")
		}
		for _, accountKey := range(sortedAccountKeys) {
			group := accountKey
			accountList := accounts[accountKey]
			//csvData = appendCSVHeader(csvData, group)
			for _, account := range(accountList) {
				log.Printf("[main] pulling data for account %s (group %s)\n", account.AccountID, group)			
				csvData, _, err = pullAWS(*awsPuller, reportfile, group, account, csvData, *monthPtr, *costTypePtr)
				if err != nil {
					log.Fatalf("[main] error pulling data: %v", err)
				}
			}
		}
	case "cm":
		cookie, err := retrieveCookie(*cookiePtr, *readcookiePtr, *cookieDbPtr)
		if err != nil {
			log.Fatalf("[main] error retrieving cookie: %v", err)
		}
		httpClient := &http.Client{}
		cmPuller := NewCMPuller(*debugPtr, httpClient, cookie)
		for _, accountKey := range(sortedAccountKeys) {
			group := accountKey
			accountList := accounts[accountKey]
			//csvData = appendCSVHeader(csvData, group)
			for _, account := range(accountList) {
				log.Printf("[main] pulling data for account %s (group %s)\n", account.AccountID, group)			
				csvData, _, err = pullCostManagement(*cmPuller, reportfile, account, csvData, *monthPtr)
				if err != nil {
					log.Fatalf("[main] error pulling data: %v", err)
				}
			}
		}
	case "crosscheck":
		log.Println("[main] note: using credentials and account from env AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for aws pull")
		if *monthPtr == "" || *costTypePtr == "" {
			log.Fatal("[main] aws mode requested, but no month and/or costtype given (use --month=yyyy-mm, --costtype=type)")
		}
		cookie, err := retrieveCookie(*cookiePtr, *readcookiePtr, *cookieDbPtr)
		if err != nil {
			log.Fatalf("[main] error retrieving cookie: %v", err)
		}
		httpClient := &http.Client{}
		cmPuller := NewCMPuller(*debugPtr, httpClient, cookie)
		for _, accountKey := range(sortedAccountKeys) {
			group := accountKey
			accountList := accounts[accountKey]
			//csvData = appendCSVHeader(csvData, group)
			for _, account := range(accountList) {
				log.Printf("[main] pulling data for account %s (group %s)\n", account.AccountID, group)
				var totalAWS float64
				_, totalAWS, err = pullAWS(*awsPuller, reportfile, group, account, nil, *monthPtr, *costTypePtr)
				if err != nil {
					log.Fatalf("[main] error pulling data: %v", err)
				}
				var totalCM float64
				csvData, totalCM, err = pullCostManagement(*cmPuller, reportfile, account, csvData, *monthPtr)
				if err != nil {
					log.Fatalf("[main] error pulling data: %v", err)
				}
				// check if totals from AWS and CM are consistent
				if math.Round(totalAWS*100)/100 != math.Round(totalCM*100)/100 {
					log.Printf("[main] error checking consistency of totals from AWS and CM for account %s: aws = %f; cm = %f", account.AccountID, totalAWS, totalCM)
					writeReport(reportfile, fmt.Sprintf("%s: error checking consistency of totals from AWS and CM: aws = %f; cm = %f", account.AccountID, totalAWS, totalCM))
				}
			}
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

func sortedKeys(m map[string][]AccountEntry) ([]string) {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	return keys
}

func retrieveCookie(cookie string, readcookie bool, cookieDbFile string) (map[string]string, error) {
	if cookie != "" {
		// cookie is given on the cli in CURL format
		log.Println("[retrieveCookie] retrieving cookies from cli")
		return deserializeCurlCookie(cookie)
	} else if readcookie {
		// cookie is to be read from Chrome's cookie database
		log.Println("[retrieveCookie] retrieving cookies from Chrome database")
		// wait for user to login
		fmt.Print("ACTION REQUIRED: please login to https://cloud.redhat.com/beta/cost-management/aws using your Chrome browser. Hit Enter when done.")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		fmt.Println("Thanks! Now retrieving cookies from Chrome..")
		cookiesCRH, err := kooky.ReadChromeCookies(cookieDbFile, "cloud.redhat.com", "", time.Time{})
		if err != nil {
			log.Fatalf("[retrieveCookie] error reading cookies from Chrome database: %v", err)
			return nil, err
		}	
		cookiesRH, err := kooky.ReadChromeCookies(cookieDbFile, ".redhat.com", "", time.Time{})
		if err != nil {
			log.Fatalf("[retrieveCookie] error reading cookies from Chrome database: %v", err)
			return nil, err
		}	
		cookiesCRH = append(cookiesCRH, cookiesRH...)
		return deserializeChromeCookie(cookiesCRH)
	} 	
	return nil, errors.New("[retrieveCookie] either --readcookie or --cookie=<cookie> needs to be given")
}

func pullAWS(awsPuller AWSPuller, reportfile *os.File, group string, account AccountEntry, csvData [][]string, month string, costType string) ([][]string, float64, error) {
	log.Printf("[pullAWS] pulling AWS data for account %s", account.AccountID)
	result, err := awsPuller.PullData(account.AccountID, month, costType)
	if err != nil {
		log.Fatalf("[pullAWS] error pulling data from AWS for account %s: %v", account.AccountID, err)
		return csvData, 0, err
	}	
	total, err := awsPuller.CheckResponseConsistency(account, result)
	if err != nil {
		log.Printf("[pullAWS] consistency check failed on response for account data %s: %v", account.AccountID, err)
		writeReport(reportfile, account.AccountID + ": " + err.Error())
	} else {
		log.Printf("[pullAWS] successful consistency check for data on account %s\n", account.AccountID)
	}
	normalized, err := awsPuller.NormalizeResponse(group, month, account.AccountID, result)
	if err != nil {
		log.Fatalf("[pullAWS] error normalizing data from AWS for account %s: %v", account.AccountID, err)
		return csvData, 0, err
	}	
	if csvData != nil {
		csvData = appendCSVData(csvData, account.AccountID, normalized)	
	}
	return csvData, total, nil
}

func pullCostManagement(cmPuller CMPuller, reportfile *os.File, account AccountEntry, csvData [][]string, month string) ([][]string, float64, error) {
	log.Printf("[pullCostManagement] pulling cost management data for account %s", account.AccountID)
	result, err := cmPuller.PullData(account.AccountID)
	if err != nil {
		log.Fatalf("[pullCostManagement] error pulling data from service: %v", err)
		return csvData, 0, err
	}
	parsed, err := cmPuller.ParseResponse(result)
	if err != nil {
		log.Fatalf("[pullCostManagement] error parsing data from service: %v", err)
		return csvData, 0, err
	}
	total, err := cmPuller.CheckResponseConsistency(account, parsed)
	if err != nil {
		log.Printf("[pullCostManagement] error checking consistency of response for account data %s: %v", account.AccountID, err)
		writeReport(reportfile, account.AccountID + " (CM): " + err.Error())
	} else {
		log.Printf("[pullCostManagement] successful consistency check for data on account %s\n", account.AccountID)
	}
	normalized, err := cmPuller.NormalizeResponse(parsed)
	if err != nil {
		log.Fatalf("[pullCostManagement] error normalizing data from service: %v", err)
		return csvData, 0, err
	}
	if csvData != nil {
		csvData = appendCSVData(csvData, account.AccountID, normalized)
	}
	return csvData, total, nil
}

func deserializeCurlCookie(curlCookie string) (map[string]string, error) {
	deserialized := make(map[string]string)
	cookieElements := strings.Split(curlCookie, "; ")
	for _, cookieStr := range(cookieElements) {
		keyValue := strings.Split(cookieStr, "=")
		if len(keyValue) < 2 {
			return nil, errors.New("[deserializecurlcookie] cookie not in correct format")
		}
		deserialized[keyValue[0]] = keyValue[1]
	}
	return deserialized, nil
}

func deserializeChromeCookie(chromeCookies []*kooky.Cookie) (map[string]string, error) {
	deserialized := make(map[string]string)
	for _, cookie := range chromeCookies {
		deserialized[cookie.Name] = cookie.Value
	}
	return deserialized, nil
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

func writeReport(outfile *os.File, data string) error {
	_, err := outfile.WriteString(data + "\n")
	if err != nil {
		log.Printf("[writereport] error writing report data to file: %v ", err)
		return err
	}
	return nil
}

func getAccountSetsFromFile(accountsFile string) (map[string][]AccountEntry, error) {
	accounts := make(map[string][]AccountEntry)
	yamlFile, err := ioutil.ReadFile(accountsFile)
	if err != nil {
			log.Printf("[getaccountsets] error reading accounts file: %v ", err)
			return nil, err
	}
	err = yaml.Unmarshal(yamlFile, accounts)
	if err != nil {
			log.Fatalf("[getaccountsets] error unmarshalling accounts file: %v", err)
			return nil, err
	}
	// set category manually on all entries
	for category, accountEntries := range accounts {
		for _, accountEntry := range accountEntries {
			accountEntry.Category = category
		}
	}
	return accounts, nil
}

func getAccountSetsFromAWS(awsPuller *AWSPuller) (map[string][]AccountEntry, error) {
	metadata, err := awsPuller.GetAWSAccountMetadata()
	if err != nil {
		log.Fatalf("[main] error getting accounts list from metadata: %v", err)
	}
	accounts := make(map[string][]AccountEntry)
	for accountID, accountMetadata := range metadata {
		if category, ok := accountMetadata[AWSTagCostpullerCategory]; ok {
			description := accountMetadata[AWSMetadataDescription]
			status := accountMetadata[AWSMetadataStatus]
			if status == "ACTIVE" {
				if _, ok := accounts[category]; !ok {
					accounts[category] = []AccountEntry{}
				}
				accounts[category] = append(accounts[category], AccountEntry{
					AccountID:        accountID,
					Standardvalue:    0,
					Deviationpercent: 0,
					Category:         category,
					Description:      description,
				})	
			}
		}
	}
	return accounts, nil	
}