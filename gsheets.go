package main

import (
	"context"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
	"log"
	"net/http"
	"time"
)

// postToGSheet creates a new sheet in a Google Sheets spreadsheet and loads it
// with the specified data.  Requests are made to the Google API using the
// specified HTTP client which has already been authenticated and authorized.
// The new sheet name is constructed based on the reference time passed in the
// last parameter.  Details such as the spreadsheet ID and sheet names are found
// in the configuration map.
func postToGSheet(sheetData []*sheets.RowData, client *http.Client, configMap map[string]string, ref time.Time) {
	srv, err := sheets.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to create Google Sheets client: %v", err)
	}

	// Construct the name for the raw data sheet using the template-name from
	// the configuration as a format specifier for time.Format()
	// (see https://pkg.go.dev/time#Layout).  Format fields (represented by
	// strings of digits) are replaced with portions of the reference time
	// value while non-digits are copied literally, so, if the template-name is
	// "Raw Data 01/2006" and the reference time is in August 2024, the result
	// will be "Raw Data 08/2024".
	newSheetName := ref.Format(getMapKeyValue(configMap, "sheetNameTemplate", "gsheet"))

	spreadsheetId := getMapKeyValue(configMap, "spreadsheetId", "gsheet")
	log.Println("Fetching Spreadsheet information")
	sheetObject, err := srv.Spreadsheets.
		Get(spreadsheetId).
		Fields("sheets/properties(gridProperties(columnCount,rowCount),sheetId,title)", "spreadsheetId").
		Do()
	if err != nil {
		log.Fatalf("Error retrieving spreadsheet: %v", err)
	}

	newDataRef := getUpdateLocation(srv, sheetObject, newSheetName, len(sheetData[0].Values), configMap)

	mainSheetName := getMapKeyValue(configMap, "mainSheetName", "gsheet")
	mainSheetID, msIndex := getSheetIDFromName(sheetObject, mainSheetName)
	if msIndex < 0 {
		log.Fatalf("Error updating spreadsheet sheet: main sheet %q not found", mainSheetName)
	}

	cells, err := srv.Spreadsheets.Values.Get(spreadsheetId, "'"+mainSheetName+"'!A1:ZZZ9999").Do()
	if err != nil {
		log.Fatalf("Error fetching main sheet (%q) values: %v", mainSheetID, err)
	}
	mainSheetRef := getNewSheetReference(cells, mainSheetID, newSheetName, len(sheetData))
	if mainSheetRef == nil {
		log.Fatalf("No reference to %q found in main sheet (%q)", newSheetName, mainSheetName)
	}
	loadNewData(srv, spreadsheetId, sheetData, newDataRef, mainSheetRef)
}

// getUpdateLocation is a helper function which returns the GridRange to
// receive the new data.  This includes looking up the existing sheet or
// creating a new one.
func getUpdateLocation(
	srv *sheets.Service,
	sheetObject *sheets.Spreadsheet,
	newSheetName string,
	newColumnCount int,
	configMap map[string]string,
) (newDataRef *sheets.GridRange) {
	_, newIndex := getSheetIDFromName(sheetObject, newSheetName)
	if newIndex < 0 {
		templateSheetName := getMapKeyValue(configMap, "templateSheetName", "gsheet")
		srcID, srcIndex := getSheetIDFromName(sheetObject, templateSheetName)
		if srcIndex < 0 {
			log.Fatalf("Error updating spreadsheet sheet: template sheet %q not found", templateSheetName)
		}

		srcColumnCount := sheetObject.Sheets[srcIndex].Properties.GridProperties.ColumnCount
		if int64(newColumnCount) >= srcColumnCount {
			log.Fatalf(
				"Unexpected column count for data:  received %d; expected fewer than %d",
				srcColumnCount,
				newColumnCount,
			)
		}

		log.Printf("Adding new sheet %q", newSheetName)
		spreadsheetId := sheetObject.SpreadsheetId
		newDataRef = createNewSheet(srv, spreadsheetId, newSheetName, srcID, int64(len(sheetObject.Sheets)))
	} else {
		log.Printf("Warning:  overwriting sheet %q", newSheetName)
		newDataRef = getDataGridRange(sheetObject.Sheets[newIndex].Properties)
	}
	return newDataRef
}

// loadNewData updates the data cells (avoiding the header row and the totals
// column) in the indicated sheet of the indicated spreadsheet from the
// provided RowData using the provided service client; it then copies a range
// of cells new sheet with the new data, and then poke the main sheet
// to get it to update its references to the new sheet.
func loadNewData(
	srv *sheets.Service,
	spreadsheetId string,
	sheetData []*sheets.RowData,
	newSheetRef *sheets.GridRange,
	mainSheetRef *sheets.GridRange,
) {
	_, err := srv.Spreadsheets.BatchUpdate(spreadsheetId, &sheets.BatchUpdateSpreadsheetRequest{
		Requests: []*sheets.Request{
			{
				UpdateCells: &sheets.UpdateCellsRequest{
					Fields: "userEnteredValue",
					Range:  newSheetRef,
					Rows:   sheetData,
				},
			},
			{
				CopyPaste: &sheets.CopyPasteRequest{
					Destination:      mainSheetRef,
					PasteOrientation: "NORMAL",
					PasteType:        "PASTE_NORMAL",
					Source:           mainSheetRef,
				},
			},
		},
	}).Do()
	if err != nil {
		log.Fatalf("Error updating sheet: %v", err)
	}
}

// createNewSheet creates a new sheet in the provided spreadsheet using the
// provided service client by duplicating the provided source sheet and
// inserting it into the spreadsheet at the indicated position with the
// provided name; it then returns the address of a GridRange describing where
// to place the new data (avoiding the header row).
func createNewSheet(srv *sheets.Service, spreadsheetId string, newSheetName string, srcID int64, position int64) *sheets.GridRange {
	buResp, err := srv.Spreadsheets.BatchUpdate(spreadsheetId, &sheets.BatchUpdateSpreadsheetRequest{
		Requests: []*sheets.Request{
			{
				DuplicateSheet: &sheets.DuplicateSheetRequest{
					NewSheetName:     newSheetName,
					SourceSheetId:    srcID,
					InsertSheetIndex: position,
				},
			},
		},
	}).Do()
	if err != nil {
		log.Fatalf("Error duplicating sheet: %v", err)
	}

	props := buResp.Replies[0].DuplicateSheet.Properties
	return getDataGridRange(props)
}

// getGridRange is a helper function which produces a GridRange describing the
// contents of a sheet, minus the header row and last column, given the sheet's
// properties object.
func getDataGridRange(props *sheets.SheetProperties) *sheets.GridRange {
	cc := props.GridProperties.ColumnCount
	rc := props.GridProperties.RowCount
	id := props.SheetId

	return &sheets.GridRange{
		EndColumnIndex:   cc - 1, // Skip "Totals" column
		EndRowIndex:      rc,
		SheetId:          id,
		StartColumnIndex: 0,
		StartRowIndex:    1, // Skip header row
	}
}

// getNewSheetReference returns a pointer to a GridRange which describes the
// cells in the provided main sheet which (indirectly) refer to the indicated
// new sheet.  This is done by locating the cell in the provided ValueRange
// which refers to the provided new sheet by name; we assume the indirect
// references are in the same column starting in the row below the matching
// cell and that there will be the provided number of rows.
func getNewSheetReference(
	cells *sheets.ValueRange,
	mainSheetID int64,
	newSheetName string,
	rowCount int,
) *sheets.GridRange {
	for r, row := range cells.Values {
		for c, cell := range row {
			if str, ok := cell.(string); ok {
				if str == newSheetName {
					msColumn := int64(c)
					msRow := int64(r + 1)
					// Indices are zero-based, starts are inclusive, ends are exclusive.
					return &sheets.GridRange{
						EndColumnIndex:   msColumn + 1,
						EndRowIndex:      msRow + int64(rowCount) + 1,
						SheetId:          mainSheetID,
						StartColumnIndex: msColumn,
						StartRowIndex:    msRow,
					}
				}
			}
		}
	}
	return nil
}

// getSheetIDFromName is a helper function which returns the sheet ID for the
// sheet (tab) with the given name in the specified spreadsheet.  Returns a
// boolean indicating if the name was not found.
func getSheetIDFromName(sheetObject *sheets.Spreadsheet, sheetName string) (int64, int) {
	for index, sheet := range sheetObject.Sheets {
		if sheet.Properties.Title == sheetName {
			return sheet.Properties.SheetId, index
		}
	}
	return -1, -1
}
