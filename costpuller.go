// Theory of Operation
//
// This tool gathers billing data from various accounts on various cloud
// providers.  Ultimately, it will support AWS, Azure, Google Cloud Platform,
// and IBM Cloud; currently, it supports only AWS.  The data gathered is either
// saved to a local file as CSV or it is loaded into a Google Sheet.
//
// The configuration for this tool is provided by a YAML file.  The file
// provides the list of cloud providers and the account IDs for each one,
// grouped by organization.  It also provides a section for configuring and
// customizing the operation of this tool.
//
// Providing Credentials
//
//  - AWS access is controlled in the conventional ways:  either via
//    environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY or via
//    ~/.aws/ config files created by the `awscli configure` command.  If using
//    the file-based credentials, you may select a specific profile.
//  - Google Sheets access is provided via OAuth 2.0.  This tool acts as an
//    OAuth client.  The client configuration is provided in the conventional
//    location (${HOME}/.config/gcloud/application_default_credentials.json)
//    and can be downloaded from https://console.developers.google.com, under
//    "Credentials").  The access token and refresh token are cached in a local
//    file.  If the token file doesn't exist, this tool prompts the user to
//    open a page in their browser (this should be done on the same machine
//    which is running this tool).  The browser will allow the user to interact
//    with Google's authentication servers, and then it will be redirected to a
//    listener provided by this tool, which allows the tool to obtain the
//    OAuth access code.  The tool then exchanges that for the tokens, which it
//    writes to the cache file.
//
// The Output
//
//    This tool collects the billing data from the cloud provider for each
//    account in the YAML file.  The data is post-processed with certain values
//    being coalesced into category values.  The result is a single row with
//    canonical columns.  The data can be output to a CSV file, or it can be
//    loaded into a Google Spreadsheet.
//
// The Google Sheets Spreadsheet Configuration & Magic
//
//    The Google spreadsheet is selected by its ID which is configured in the
//    "gsheet" subsection of the "configuration" section of the YAML file with
//    the key, "spreadsheetId".  The value comes from the URL used to view the
//    spreadsheet.
//
//    The raw data is loaded into a new "tab" or "sheet" in the spreadsheet.
//    The sheet is named by expanding a name-template configured in the YAML
//    file with the key "sheetNameTemplate".  Digits in the value are replaced
//    with elements of the reference time timestamp, as described in
//    https://pkg.go.dev/time#Layout: for instance, in "Raw Data 01/2006", the
//    "01" would be replaced by the two-digit numerical month and "2006" would
//    be replaced by the four-digit year.  The reference time can be specified
//    with the `-month` command line option, as "-month 2024-08", but it
//    defaults to the month previous to the current one.
//
//    The target sheet for the raw data is created by copying an existing
//    template sheet in the spreadsheet whose name is configured with the key,
//    "templateSheetName".  The first row of this sheet is reserved for column
//    headers, and so the data is written starting at the second row.  The last
//    column of the sheet is reserved for row totals, and so the data rows must
//    fit in the preceding columns.  Also, in order for the tool to correctly
//    determine the "totals" column, the template sheet must not include any
//    columns after the "totals" column (any extra columns should be explicitly
//    deleted).
//
//    Finally, the tool expects that the spreadsheet contains a "main sheet"
//    which references the raw data sheets.  This sheet must be specified in
//    the using YAML file using the key, "mainSheetName".  Unfortunately,
//    Google Sheets seems to have a mal-feature which results in situations
//    where references between sheets are not updated reliably.  For instance,
//    creating a new sheet or, in many cases, even just updating it, will not
//    refresh a reference to it in another sheet.  The accepted workaround for
//    this is to copy and paste the cell references over themselves.  To effect
//    this, the tool expects that there is a cell in the main sheet which
//    contains the name of the raw data sheet and which is used for indirect
//    lookups in the raw data sheet, moreover that the formulas containing the
//    indirect references are found in the column immediately below this cell
//    and that there is one entry for each row of data.  The tool will locate
//    the cell which contains the sheet reference, copy the appropriate number
//    of cells below it, and paste those values over themselves.  The paste
//    operation is non-destructive, so it is not a problem if it encompasses
//    unrelated cells, but it must include all cells with references to the
//    new sheet.

package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/browserutils/kooky"
	"github.com/browserutils/kooky/browser/chrome"
	"google.golang.org/api/sheets/v4"
	"gopkg.in/yaml.v2"
)

type CommandLineOptions struct {
	modePtr           *string
	debugPtr          *bool
	awsWriteTagsPtr   *bool
	awsCheckTagsPtr   *bool
	accountsFilePtr   *string
	taggedAccountsPtr *bool
	monthPtr          *string
	costTypePtr       *string
	cookiePtr         *string
	readcookiePtr     *bool
	cookieDbPtr       *string
	csvfilePtr        *string
	reportFilePtr     *string
	outputTypePtr     *string
}

type AccountsFile struct {
	Configuration map[string]map[string]string         `yaml:"configuration"`
	Providers     map[string]map[string][]AccountEntry `yaml:"cloud_providers"`
}

// AccountEntry describes an account with metadata.
type AccountEntry struct {
	AccountID        string  `yaml:"accountid"`
	Standardvalue    float64 `yaml:"standardvalue"`
	Deviationpercent int     `yaml:"deviationpercent"`
	Category         string  `yaml:"category"`
	Description      string  `yaml:"description"`
}

func main() {
	log.Println("[main] costpuller starting..")
	nowTime := time.Now()
	lastMonth := time.Date(nowTime.Year(), nowTime.Month()-1, 1, 0, 0, 0, 0, nowTime.Location())
	nowStr := nowTime.Format("20060102150405")
	defaultMonth := lastMonth.Format("2006-01")
	defaultCsvFile := fmt.Sprintf("output-%s.csv", defaultMonth)
	defaultReportFile := fmt.Sprintf("report-%s.txt", nowStr)
	options := CommandLineOptions{
		accountsFilePtr:   flag.String("accounts", "accounts.yaml", "file to read accounts list from"),
		awsCheckTagsPtr:   flag.Bool("checktags", false, "checks all AWS accounts available for correct tag setting."),
		awsWriteTagsPtr:   flag.Bool("awswritetags", false, "write tags to AWS accounts (USE WITH CARE!)"),
		cookieDbPtr:       flag.String("cookiedb", getDefaultCookieStore(), `path to Chrome cookies database file, only for "cm" or "crosscheck" modes`),
		cookiePtr:         flag.String("cookie", "", `access cookie for cost management system in curl serialized format, only for "cm" or "crosscheck" modes`),
		costTypePtr:       flag.String("costtype", "UnblendedCost", `cost type to pull, only for "aws" or "crosscheck" modes, one of "AmortizedCost", "BlendedCost", "NetAmortizedCost", "NetUnblendedCost", "NormalizedUsageAmount", "UnblendedCost", or "UsageQuantity"`),
		csvfilePtr:        flag.String("csv", defaultCsvFile, "output file for csv data"),
		debugPtr:          flag.Bool("debug", false, "outputs debug info"),
		modePtr:           flag.String("mode", "aws", `run mode, needs to be one of "aws", "cm" or "crosscheck"`),
		monthPtr:          flag.String("month", defaultMonth, `context month in format yyyy-mm, only for "aws" or "crosscheck" modes`),
		outputTypePtr:     flag.String("output", "gsheet", `output destination, needs to be one of "csv" or "gsheet"`),
		readcookiePtr:     flag.Bool("readcookie", true, `reads the cookie from the Chrome cookies database, only for "cm" or "crosscheck" modes`),
		reportFilePtr:     flag.String("report", defaultReportFile, "output file for data consistency report"),
		taggedAccountsPtr: flag.Bool("taggedaccounts", false, "use the AWS tags as account list source"),
	}
	flag.Parse()

	accountsFile, err := loadAccountsFile(*options.accountsFilePtr)
	if err != nil {
		log.Fatalf("[main] error loading accounts file: %v", err)
	}
	if len(accountsFile.Configuration) == 0 {
		log.Fatalf("[main] error in accounts file: empty or missing \"configuration\" section")
	}
	if len(accountsFile.Providers) == 0 {
		log.Fatalf("[main] error in accounts file: empty or missing \"cloud_providers\" section")
	}

	awsConfig := getMapKeyValue(accountsFile.Configuration, "aws", "configuration")
	awsProfile := awsConfig["profile"]
	if awsProfile == "" {
		awsProfile = "default"
		log.Printf(
			"[main] no \"profile\" key found in the \"aws\" section of the configuration file; "+
				"using AWS credentials profile %q",
			awsProfile,
		)
	}
	awsPuller := NewAwsPuller(awsProfile, *options.debugPtr)

	if *options.awsWriteTagsPtr {
		writeAwsTags(awsPuller, options)
		os.Exit(0)
	}

	if *options.awsCheckTagsPtr {
		checkAwsTags(awsPuller)
		os.Exit(0)
	}

	reportFile := getReportFile(options)
	defer closeFile(reportFile)

	awsAccounts, sortedAccountKeys := awsPuller.getAwsAccounts(accountsFile, options)

	switch *options.modePtr {
	case "aws":
		var client *http.Client
		var outfile *os.File

		refTime, err := time.Parse("2006-01", *options.monthPtr)
		if err != nil {
			log.Fatalf("[main] error parsing month value, %q: %v", *options.monthPtr, err)
		}

		if *options.outputTypePtr == "csv" {
			outfile = getCsvFile(options)
			defer closeFile(outfile)
		} else if *options.outputTypePtr == "gsheet" {
			oauthConfig := getMapKeyValue(accountsFile.Configuration, "oauth", "configuration")
			client = getGoogleOAuthHttpClient(oauthConfig)
		} else {
			log.Fatalf("[main] Unexpected value for output type, %q", *options.outputTypePtr)
		}

		sheetData := awsPuller.pullAwsByAccount(awsAccounts, sortedAccountKeys, options, reportFile)

		if *options.outputTypePtr == "csv" {
			err = writeCsvFromSheet(outfile, sheetData)
			if err != nil {
				log.Fatalf("[main] error writing to output file: %v", err)
			}
		} else if *options.outputTypePtr == "gsheet" {
			gsheetConfig := getMapKeyValue(accountsFile.Configuration, "gsheet", "base")
			postToGSheet(sheetData, client, gsheetConfig, refTime)
		}

	case "cm":
		var csvData [][]string
		cookie, err := retrieveCookie(*options.cookiePtr, *options.readcookiePtr, *options.cookieDbPtr)
		if err != nil {
			log.Fatalf("[main] error retrieving cookie: %v", err)
		}
		outfile := getCsvFile(options)
		defer closeFile(outfile)
		cmPuller := NewCmPuller(cookie, *options.debugPtr)
		for _, accountKey := range sortedAccountKeys {
			group := accountKey
			accountList := awsAccounts[accountKey]
			for _, account := range accountList {
				log.Printf("[main] pulling data for account %s (group %s)\n", account.AccountID, group)
				csvData, _, err = pullCostManagement(*cmPuller, reportFile, account, csvData)
				if err != nil {
					log.Fatalf("[main] error pulling data: %v", err)
				}
			}
		}
		err = writeCsv(outfile, csvData)
		if err != nil {
			log.Fatalf("[main] error writing to output file: %v", err)
		}
	case "crosscheck":
		var csvData [][]string
		if *options.monthPtr == "" || *options.costTypePtr == "" {
			log.Fatal("[main] aws mode requested, but no month and/or cost type given (use --month=yyyy-mm, --costtype=type)")
		}
		outfile := getCsvFile(options)
		defer closeFile(outfile)
		cookie, err := retrieveCookie(*options.cookiePtr, *options.readcookiePtr, *options.cookieDbPtr)
		if err != nil {
			log.Fatalf("[main] error retrieving cookie: %v", err)
		}
		cmPuller := NewCmPuller(cookie, *options.debugPtr)
		for _, accountKey := range sortedAccountKeys {
			group := accountKey
			accountList := awsAccounts[accountKey]
			for _, account := range accountList {
				log.Printf("[main] pulling data for account %s (group %s)\n", account.AccountID, group)
				var totalAws float64
				_, totalAws, err = awsPuller.pullAwsAccount(
					account,
					group,
					*options.monthPtr,
					*options.costTypePtr,
					reportFile,
				)
				if err != nil {
					log.Fatalf("[main] error pulling data: %v", err)
				}
				var totalCM float64
				csvData, totalCM, err = pullCostManagement(*cmPuller, reportFile, account, csvData)
				if err != nil {
					log.Fatalf("[main] error pulling data: %v", err)
				}
				// check if totals from AWS and CM are consistent
				if math.Round(totalAws*100)/100 != math.Round(totalCM*100)/100 {
					log.Printf(
						"[main] error checking consistency of totals from AWS and CM for account %s: aws = %f; cm = %f",
						account.AccountID,
						totalAws,
						totalCM,
					)
					writeReport(reportFile, fmt.Sprintf(
						"%s: error checking consistency of totals from AWS and CM: aws = %f; cm = %f",
						account.AccountID,
						totalAws,
						totalCM,
					))
				}
			}
		}
		err = writeCsv(outfile, csvData)
		if err != nil {
			log.Fatalf("[main] error writing to output file: %v", err)
		}
	}

	log.Println("[main] operation done")
}

// getDefaultCookieStore encapsulates the platform-specific location of the
// default browser cookie database file.
//
// TODO:  kooky.FindAllCookieStores() can handle this for us.
func getDefaultCookieStore() string {
	defaultCookieDb, _ := os.UserConfigDir()
	if runtime.GOOS == "linux" {
		defaultCookieDb = filepath.Join(defaultCookieDb, "google-chrome")
	} else if runtime.GOOS == "darwin" {
		defaultCookieDb = filepath.Join(defaultCookieDb, "Google/Chrome")
	} else {
		log.Printf("[main] unexpected platform:  %q\n", runtime.GOOS)
	}
	defaultCookieDb = filepath.Join(defaultCookieDb, "Default/Cookies")
	return defaultCookieDb
}

func (a *AwsPuller) getAwsAccounts(
	accountsFile AccountsFile,
	options CommandLineOptions,
) (accounts map[string][]AccountEntry, keys []string) {
	//var accounts map[string][]AccountEntry
	if *options.taggedAccountsPtr {
		a, err := getAccountSetsFromAws(a)
		if err != nil {
			log.Fatalf("[getAwsAccounts] error getting accounts list: %v", err)
		}
		accounts = a
	} else {
		accounts = getMapKeyValue(accountsFile.Providers, "aws", "cloud_providers")
	}
	if len(accounts) == 0 {
		fmt.Println("[getAwsAccounts] Warning:  No AWS accounts found!")
	}
	return accounts, sortedKeys(accounts)
}

func (a *AwsPuller) pullAwsByAccount(
	accounts map[string][]AccountEntry,
	sortedAccountKeys []string,
	options CommandLineOptions,
	reportFile *os.File,
) (sheetData []*sheets.RowData) {
	if *options.monthPtr == "" || *options.costTypePtr == "" {
		log.Fatal("[pullAwsByAccount] aws mode requested, but no month and/or cost type given (use --month=yyyy-mm, --costtype=type)")
	}
	for _, group := range sortedAccountKeys {
		accountList := accounts[group]
		if len(accountList) == 0 {
			log.Printf("[pullAwsByAccount] Warning: no accounts found in group %q!", group)
		}
		for _, account := range accountList {
			log.Printf("[pullAwsByAccount] pulling data for account %s (group %s)\n", account.AccountID, group)
			rowData, _, err := a.pullAwsAccount(
				account,
				group,
				*options.monthPtr,
				*options.costTypePtr,
				reportFile,
			)
			if err != nil {
				log.Fatalf("[pullAwsByAccount] error pulling data: %v", err)
			}
			sheetData = append(sheetData, rowData)
		}
	}
	return
}

func writeAwsTags(awsPuller *AwsPuller, options CommandLineOptions) {
	accountsFile, err := loadAccountsFile(*options.accountsFilePtr)
	if err != nil {
		log.Fatalf("[writeAwsTags] error getting accounts list: %v", err)
	}
	accounts := getMapKeyValue(accountsFile.Providers, "aws", "cloud_providers")
	err = awsPuller.WriteAwsTags(accounts)
	if err != nil {
		log.Fatalf("[writeAwsTags] error writing account tag: %v", err)
	}
}

func checkAwsTags(awsPuller *AwsPuller) {
	log.Println("[checkAwsTags] checking tags on AWS")
	_, err := getAccountSetsFromAws(awsPuller)
	if err != nil {
		log.Fatalf("[checkAwsTags] error getting accounts list: %v", err)
	}
}

func getCsvFile(options CommandLineOptions) *os.File {
	outfile, err := os.Create(*options.csvfilePtr)
	if err != nil {
		log.Fatalf("[getCsvFile] error creating output file: %v", err)
	}
	log.Printf("[getCsvFile] using csv output file %s\n", *options.csvfilePtr)
	return outfile
}

func getReportFile(options CommandLineOptions) *os.File {
	reportFile, err := os.Create(*options.reportFilePtr)
	if err != nil {
		log.Fatalf("[getReportFile] error creating report file: %v", err)
	}
	log.Printf("[getReportFile] using report output file %s\n", *options.reportFilePtr)
	return reportFile
}

func sortedKeys(m map[string][]AccountEntry) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func retrieveCookie(cookie string, readCookie bool, cookieDbFile string) (map[string]string, error) {
	if cookie != "" {
		// cookie is given on the cli in CURL format
		log.Println("[retrieveCookie] retrieving cookies from cli")
		return deserializeCurlCookie(cookie)
	} else if readCookie {
		// cookie is to be read from Chrome's cookie database
		log.Println("[retrieveCookie] retrieving cookies from Chrome database")
		// wait for user to login
		fmt.Print("ACTION REQUIRED: please login to https://cloud.redhat.com/beta/cost-management/aws using your Chrome browser. Hit Enter when done.")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		fmt.Println("Thanks! Now retrieving cookies from Chrome..")
		crhCookies, err := chrome.ReadCookies(cookieDbFile, kooky.Domain("cloud.redhat.com"))
		if err != nil {
			log.Fatalf("[retrieveCookie] error reading cookies from Chrome database: %v", err)
			return nil, err
		}
		rhCookies, err := chrome.ReadCookies(cookieDbFile, kooky.DomainHasSuffix(".redhat.com"))
		if err != nil {
			log.Fatalf("[retrieveCookie] error reading cookies from Chrome database: %v", err)
			return nil, err
		}
		return deserializeChromeCookie(append(crhCookies, rhCookies...))
	}
	return nil, errors.New("[retrieveCookie] either --readcookie or --cookie=<cookie> needs to be given")
}

func (a *AwsPuller) pullAwsAccount(
	account AccountEntry,
	group string,
	month string,
	costType string,
	reportFile *os.File,
) (normalized *sheets.RowData, total float64, err error) {
	result, err := a.PullData(account.AccountID, month, costType)
	if err != nil {
		log.Fatalf("[pullAwsAccount] error pulling data from AWS for account %s: %v", account.AccountID, err)
	}
	total, err = a.CheckResponseConsistency(account, result)
	if err != nil {
		log.Printf(
			"[pullAwsAccount] consistency check failed on response for account data %s: %v",
			account.AccountID,
			err,
		)
		writeReport(reportFile, account.AccountID+": "+err.Error())
	}
	normalized, err = a.NormalizeResponse(group, month, account.AccountID, result)
	if err != nil {
		log.Fatalf("[pullAwsAccount] error normalizing data from AWS for account %s: %v", account.AccountID, err)
	}
	return
}

func pullCostManagement(
	cmPuller CmPuller,
	reportFile *os.File,
	account AccountEntry,
	csvData [][]string,
) ([][]string, float64, error) {
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
		log.Printf(
			"[pullCostManagement] error checking consistency of response for account data %s: %v",
			account.AccountID,
			err,
		)
		writeReport(reportFile, account.AccountID+" (CM): "+err.Error())
	} else {
		log.Printf("[pullCostManagement] successful consistency check for data on account %s\n", account.AccountID)
	}
	normalized, err := cmPuller.NormalizeResponse(parsed)
	if err != nil {
		log.Fatalf("[pullCostManagement] error normalizing data from service: %v", err)
		return csvData, 0, err
	}
	log.Printf("[pullCostManagement] appended data for account %s\n", account.AccountID)
	csvData = append(csvData, normalized)
	return csvData, total, nil
}

func deserializeCurlCookie(curlCookie string) (map[string]string, error) {
	deserialized := make(map[string]string)
	cookieElements := strings.Split(curlCookie, "; ")
	for _, cookieStr := range cookieElements {
		keyValue := strings.Split(cookieStr, "=")
		if len(keyValue) < 2 {
			return nil, errors.New("[deserializeCurlCookie] cookie not in correct format")
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

func writeCsv(outfile *os.File, data [][]string) error {
	writer := csv.NewWriter(outfile)
	defer writer.Flush()
	for _, value := range data {
		err := writer.Write(value)
		if err != nil {
			log.Printf("[writeCsv] error writing csv data to file: %v ", err)
			return err
		}
	}
	return nil
}

func writeCsvFromSheet(outfile *os.File, data []*sheets.RowData) error {
	writer := csv.NewWriter(outfile)
	defer writer.Flush()
	for _, row := range data {
		rowData := make([]string, len(row.Values))
		for i, cell := range row.Values {
			var cellData string
			if cell.UserEnteredValue.StringValue != nil {
				cellData = *cell.UserEnteredValue.StringValue
			} else if cell.UserEnteredValue.NumberValue != nil {
				cellData = fmt.Sprintf("%f", *cell.UserEnteredValue.NumberValue)
			} else {
				log.Fatalf("Unexpected sheet cell value:  %v", cell.UserEnteredValue)
			}
			rowData[i] = cellData
		}
		err := writer.Write(rowData)
		if err != nil {
			log.Printf("[writeCsvFromSheet] error writing csv data to file: %v ", err)
			return err
		}
	}
	return nil
}

func writeReport(outfile *os.File, data string) {
	_, err := outfile.WriteString(data + "\n")
	if err != nil {
		log.Printf("[writeReport] error writing report data to file: %v ", err)
	}
}

func loadAccountsFile(accountsFileName string) (accountsFile AccountsFile, err error) {
	yamlFile, err := os.ReadFile(accountsFileName)
	if err != nil {
		return accountsFile, fmt.Errorf("[loadAccountsFile] error loading accounts file: %v", err)
	}
	accountsFile = AccountsFile{
		Configuration: make(map[string]map[string]string),
		Providers:     make(map[string]map[string][]AccountEntry),
	}
	err = yaml.Unmarshal(yamlFile, accountsFile)
	if err != nil {
		return accountsFile, fmt.Errorf("[loadAccountsFile] error unmarshalling accounts file: %v", err)
	}
	// set category manually on all entries
	for _, group := range accountsFile.Providers {
		for category, accountEntries := range group {
			for _, accountEntry := range accountEntries {
				accountEntry.Category = category
			}
		}
	}
	return
}

func getAccountSetsFromAws(awsPuller *AwsPuller) (map[string][]AccountEntry, error) {
	log.Println("[getAccountSetsFromAws] initiating account metadata pull")
	metadata, err := awsPuller.GetAwsAccountMetadata()
	if err != nil {
		log.Fatalf("[getAccountSetsFromAws] error getting accounts list from metadata: %v", err)
	}
	log.Println("[getAccountSetsFromAws] processing account metadata pull")
	accounts := make(map[string][]AccountEntry)
	for accountID, accountMetadata := range metadata {
		if category, ok := accountMetadata[AwsTagCostpullerCategory]; ok {
			description := accountMetadata[AwsMetadataDescription]
			log.Printf("tagged category (\"%s\") found for account %s (\"%s\")", category, accountID, description)
			status := accountMetadata[AwsMetadataStatus]
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
		} else {
			// account without category tag
			log.Printf(
				"ERROR: account %s does not have an aws tag set for category (\"%s\")",
				accountID,
				accountMetadata[AwsMetadataDescription],
			)
		}
	}
	return accounts, nil
}

// closeFile is a helper function which allows closing a file to be deferred
// and which ignores any errors.
func closeFile(filename *os.File) {
	_ = filename.Close()
}

// getMapKeyValue is a generic helper function which fetches a value from the
// given key in the given map; if the key is not in the map, the program exits
// with an error citing the supplied section string.
func getMapKeyValue[V any](
	configMap map[string]V,
	key string,
	section string,
) (value V) {
	if value, ok := configMap[key]; ok {
		return value
	}
	log.Fatalf("Key %q is missing from the %q section of the configuration", key, section)
	return
}
