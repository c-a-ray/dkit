package ops

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/c-a-ray/dkit/internal/core"
)

// NPI API base URL
const npiAPIBase = "https://npiregistry.cms.hhs.gov/api/"

// ValidateNPI checks whether an NPI number has valid format and passes the
// Luhn check (ISO/IEC 7812, with the 80840 prefix for health applications).
func ValidateNPI(npi string) bool {
	if len(npi) != 10 {
		return false
	}
	for _, c := range npi {
		if c < '0' || c > '9' {
			return false
		}
	}
	// Prefix with 80840 per CMS spec, then run Luhn on the 15-digit string.
	prefixed := "80840" + npi
	return luhn(prefixed)
}

func luhn(s string) bool {
	sum := 0
	double := false
	for i := len(s) - 1; i >= 0; i-- {
		d := int(s[i] - '0')
		if double {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		double = !double
	}
	return sum%10 == 0
}

// LookupOpts holds query parameters for an NPI registry lookup.
type LookupOpts struct {
	Number             string
	EnumerationType    string
	TaxonomyDesc       string
	FirstName          string
	UseFirstNameAlias  bool
	LastName           string
	OrganizationName   string
	AddressPurpose     string
	City               string
	State              string
	PostalCode         string
	CountryCode        string
	Limit              int
	Skip               int
}

// NPIResult is the top-level API response.
type NPIResult struct {
	ResultCount int          `json:"result_count"`
	Results     []NPIRecord `json:"results"`
}

// NPIRecord is a single provider record.
type NPIRecord struct {
	Number            string         `json:"number"`
	EnumerationType   string         `json:"enumeration_type"`
	Basic             NPIBasic       `json:"basic"`
	Addresses         []NPIAddress   `json:"addresses"`
	Taxonomies        []NPITaxonomy  `json:"taxonomies"`
	OtherNames        []NPIOtherName `json:"other_names"`
	Identifiers       []any          `json:"identifiers"`
	Endpoints         []any          `json:"endpoints"`
	PracticeLocations []any          `json:"practiceLocations"`
}

type NPIBasic struct {
	FirstName         string `json:"first_name"`
	MiddleName        string `json:"middle_name"`
	LastName          string `json:"last_name"`
	Credential        string `json:"credential"`
	Sex               string `json:"sex"`
	SoleProprietor    string `json:"sole_proprietor"`
	Status            string `json:"status"`
	EnumerationDate   string `json:"enumeration_date"`
	LastUpdated       string `json:"last_updated"`
	CertificationDate string `json:"certification_date"`
	// Organization fields (NPI-2)
	OrganizationName       string `json:"organization_name"`
	OrganizationalSubpart  string `json:"organizational_subpart"`
	AuthorizedOfficialName string `json:"authorized_official_last_name"`
}

type NPIAddress struct {
	AddressPurpose  string `json:"address_purpose"`
	AddressType     string `json:"address_type"`
	Address1        string `json:"address_1"`
	Address2        string `json:"address_2"`
	City            string `json:"city"`
	State           string `json:"state"`
	PostalCode      string `json:"postal_code"`
	CountryCode     string `json:"country_code"`
	CountryName     string `json:"country_name"`
	TelephoneNumber string `json:"telephone_number"`
	FaxNumber       string `json:"fax_number"`
}

type NPITaxonomy struct {
	Code          string `json:"code"`
	Desc          string `json:"desc"`
	License       string `json:"license"`
	Primary       bool   `json:"primary"`
	State         string `json:"state"`
	TaxonomyGroup string `json:"taxonomy_group"`
}

type NPIOtherName struct {
	Code       string `json:"code"`
	FirstName  string `json:"first_name"`
	MiddleName string `json:"middle_name"`
	LastName   string `json:"last_name"`
	Type       string `json:"type"`
}

// LookupNPI queries the NPI registry API and returns parsed results.
func LookupNPI(o LookupOpts) (*NPIResult, error) {
	params := url.Values{}
	params.Set("version", "2.1")

	if o.Number != "" {
		params.Set("number", o.Number)
	}
	if o.EnumerationType != "" {
		params.Set("enumeration_type", o.EnumerationType)
	}
	if o.TaxonomyDesc != "" {
		params.Set("taxonomy_description", o.TaxonomyDesc)
	}
	if o.FirstName != "" {
		params.Set("first_name", o.FirstName)
	}
	if o.UseFirstNameAlias {
		params.Set("use_first_name_alias", "True")
	}
	if o.LastName != "" {
		params.Set("last_name", o.LastName)
	}
	if o.OrganizationName != "" {
		params.Set("organization_name", o.OrganizationName)
	}
	if o.AddressPurpose != "" {
		params.Set("address_purpose", o.AddressPurpose)
	}
	if o.City != "" {
		params.Set("city", o.City)
	}
	if o.State != "" {
		params.Set("state", o.State)
	}
	if o.PostalCode != "" {
		params.Set("postal_code", o.PostalCode)
	}
	if o.CountryCode != "" {
		params.Set("country_code", o.CountryCode)
	}
	if o.Limit > 0 {
		params.Set("limit", strconv.Itoa(o.Limit))
	}
	if o.Skip > 0 {
		params.Set("skip", strconv.Itoa(o.Skip))
	}

	reqURL := npiAPIBase + "?" + params.Encode()

	resp, err := http.Get(reqURL)
	if err != nil {
		return nil, fmt.Errorf("NPI API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("NPI API returned status %d: %s", resp.StatusCode, string(body))
	}

	var result NPIResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse NPI API response: %w", err)
	}

	return &result, nil
}

// NPIColOpts configures the npi col command.
type NPIColOpts struct {
	Column  string
	Summary bool
	Config  *core.Config
}

// NPIColValidate reads a column from CSV files, validates each value as an NPI,
// and prints either a detail table or a summary percentage.
func NPIColValidate(files []string, o NPIColOpts) (int, error) {
	if len(files) == 0 {
		return 0, errors.New("no files")
	}

	freq := map[string]int{}

	for _, path := range files {
		rc, err := core.OpenWithEncoding(path, o.Config.Encoding)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] cannot read %s: %v\n", path, err)
			continue
		}
		r := core.SkipLines(rc, o.Config.SkipStart, o.Config.SkipEnd)
		cr := core.NewCSVReader(r, o.Config)

		var idx int
		if o.Config.NoHeader {
			i, err := strconv.Atoi(o.Column)
			if err != nil || i < 0 {
				rc.Close()
				return 0, fmt.Errorf("--noHeader requires numeric column index, got %q", o.Column)
			}
			idx = i
		} else {
			hdr, err := cr.Read()
			if err == io.EOF {
				fmt.Fprintf(os.Stderr, "[WARN] %s is empty\n", path)
				rc.Close()
				continue
			} else if err != nil {
				fmt.Fprintf(os.Stderr, "[WARN] %s: %v\n", path, err)
				rc.Close()
				continue
			}
			idx = -1
			for i, h := range hdr {
				if h == o.Column {
					idx = i
					break
				}
			}
			if idx < 0 {
				fmt.Fprintf(os.Stderr, "[WARN] %s: header %q not found\n", path, o.Column)
				rc.Close()
				continue
			}
		}

		for {
			rec, err := cr.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "[WARN] %s: %v\n", path, err)
				break
			}
			if idx >= len(rec) {
				continue
			}
			v := strings.TrimSpace(rec[idx])
			freq[v]++
		}
		rc.Close()
	}

	if len(freq) == 0 {
		fmt.Fprintln(os.Stderr, "No values found.")
		return 0, nil
	}

	// Sort by frequency descending, then alphabetically.
	type entry struct {
		val   string
		count int
		valid bool
	}
	entries := make([]entry, 0, len(freq))
	for v, c := range freq {
		entries = append(entries, entry{v, c, ValidateNPI(v)})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count == entries[j].count {
			return entries[i].val < entries[j].val
		}
		return entries[i].count > entries[j].count
	})

	// Compute totals.
	var totalRows, validRows, emptyRows int
	for _, e := range entries {
		totalRows += e.count
		if e.val == "" {
			emptyRows += e.count
			continue
		}
		if e.valid {
			validRows += e.count
		}
	}
	nonEmptyRows := totalRows - emptyRows
	invalidRows := nonEmptyRows - validRows

	if o.Summary {
		fmt.Printf("Total rows:    %d\n", totalRows)
		if emptyRows > 0 {
			fmt.Printf("Empty:         %d\n", emptyRows)
		}
		fmt.Printf("Non-empty:     %d\n", nonEmptyRows)
		if nonEmptyRows > 0 {
			pct := 100.0 * float64(validRows) / float64(nonEmptyRows)
			fmt.Printf("Valid NPIs:    %d (%.1f%%)\n", validRows, pct)
			fmt.Printf("Invalid NPIs:  %d (%.1f%%)\n", invalidRows, 100.0-pct)
		}
		if invalidRows > 0 {
			return invalidRows, nil
		}
		return 0, nil
	}

	// Detail mode: print table.
	// Compute column width for value.
	valWidth := 5 // minimum "VALUE"
	for _, e := range entries {
		if len(e.val) > valWidth {
			valWidth = len(e.val)
		}
	}
	if valWidth > 40 {
		valWidth = 40
	}

	fmt.Printf("%-*s  %6s  %s\n", valWidth, "VALUE", "COUNT", "VALID")
	for _, e := range entries {
		display := e.val
		if display == "" {
			display = "<EMPTY>"
		}
		if len(display) > valWidth {
			display = display[:valWidth-1] + "…"
		}
		validStr := "yes"
		if e.val == "" {
			validStr = "-"
		} else if !e.valid {
			validStr = "no"
		}
		fmt.Printf("%-*s  %6d  %s\n", valWidth, display, e.count, validStr)
	}

	fmt.Fprintf(os.Stderr, "\n%d unique values, %d rows. Valid: %d, Invalid: %d",
		len(entries), totalRows, validRows, invalidRows)
	if nonEmptyRows > 0 {
		fmt.Fprintf(os.Stderr, " (%.1f%% valid)", 100.0*float64(validRows)/float64(nonEmptyRows))
	}
	fmt.Fprintln(os.Stderr)

	return invalidRows, nil
}

// PrintNPIResults writes a human-readable summary of NPI lookup results to stdout.
func PrintNPIResults(result *NPIResult) {
	if result.ResultCount == 0 {
		fmt.Fprintln(os.Stderr, "No results found.")
		return
	}
	fmt.Fprintf(os.Stderr, "%d result(s) found.\n\n", result.ResultCount)

	for i, r := range result.Results {
		if i > 0 {
			fmt.Println(strings.Repeat("─", 60))
		}
		fmt.Printf("NPI:  %s  (%s)\n", r.Number, r.EnumerationType)

		b := r.Basic
		if b.OrganizationName != "" {
			fmt.Printf("Org:  %s\n", b.OrganizationName)
		}
		if b.LastName != "" {
			name := b.LastName
			if b.FirstName != "" {
				name = b.FirstName + " " + name
			}
			if b.MiddleName != "" {
				name = b.FirstName + " " + b.MiddleName + " " + b.LastName
			}
			if b.Credential != "" {
				name += ", " + b.Credential
			}
			fmt.Printf("Name: %s\n", name)
		}

		statusLabel := b.Status
		switch b.Status {
		case "A":
			statusLabel = "Active"
		case "D":
			statusLabel = "Deactivated"
		}
		fmt.Printf("Status: %s\n", statusLabel)
		if b.EnumerationDate != "" {
			fmt.Printf("Enumeration date: %s\n", b.EnumerationDate)
		}

		for _, t := range r.Taxonomies {
			primary := ""
			if t.Primary {
				primary = " (primary)"
			}
			fmt.Printf("Taxonomy: %s – %s%s\n", t.Code, t.Desc, primary)
			if t.License != "" {
				fmt.Printf("  License: %s (%s)\n", t.License, t.State)
			}
		}

		for _, a := range r.Addresses {
			fmt.Printf("Address (%s): %s", a.AddressPurpose, a.Address1)
			if a.Address2 != "" {
				fmt.Printf(", %s", a.Address2)
			}
			fmt.Printf(", %s, %s %s\n", a.City, a.State, a.PostalCode)
			if a.TelephoneNumber != "" {
				fmt.Printf("  Phone: %s\n", a.TelephoneNumber)
			}
		}
	}
}
