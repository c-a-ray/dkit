package cli

import (
	"fmt"
	"os"

	"github.com/c-a-ray/dkit/internal/core"
	"github.com/c-a-ray/dkit/internal/ops"
	"github.com/spf13/cobra"
)

func addNPICmd(parent *cobra.Command, cfg *core.Config) {
	npiCmd := &cobra.Command{
		Use:   "npi",
		Short: "NPI registry operations",
	}

	npiCmd.AddCommand(newNPIValidCmd())
	npiCmd.AddCommand(newNPILookupCmd())
	npiCmd.AddCommand(newNPIColCmd(cfg))

	parent.AddCommand(npiCmd)
}

func newNPIValidCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "valid <npi>",
		Short: "Validate an NPI number (format + check digit)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			npi := args[0]
			if ops.ValidateNPI(npi) {
				fmt.Printf("%s is valid\n", npi)
				return nil
			}
			fmt.Printf("%s is not valid\n", npi)
			os.Exit(2)
			return nil
		},
	}
}

func newNPILookupCmd() *cobra.Command {
	var o ops.LookupOpts

	cmd := &cobra.Command{
		Use:   "lookup",
		Short: "Search the NPI registry",
		Long: `Query the NPI registry (npiregistry.cms.hhs.gov) by any combination
of search parameters.

Examples:
  dkit npi lookup --number 1164905659
  dkit npi lookup --firstName John --lastName Smith --state NY
  dkit npi lookup --organizationName "Mayo Clinic" --state MN`,
		RunE: func(cmd *cobra.Command, args []string) error {
			result, err := ops.LookupNPI(o)
			if err != nil {
				return err
			}
			ops.PrintNPIResults(result)
			if result.ResultCount == 0 {
				os.Exit(2)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&o.Number, "number", "", "NPI number")
	cmd.Flags().StringVar(&o.EnumerationType, "enumerationType", "", "NPI-1 (individual) or NPI-2 (organization)")
	cmd.Flags().StringVar(&o.TaxonomyDesc, "taxonomyDescription", "", "taxonomy description")
	cmd.Flags().StringVar(&o.FirstName, "firstName", "", "provider first name")
	cmd.Flags().BoolVar(&o.UseFirstNameAlias, "useFirstNameAlias", false, "include first name aliases")
	cmd.Flags().StringVar(&o.LastName, "lastName", "", "provider last name")
	cmd.Flags().StringVar(&o.OrganizationName, "organizationName", "", "organization name")
	cmd.Flags().StringVar(&o.AddressPurpose, "addressPurpose", "", "LOCATION or MAILING")
	cmd.Flags().StringVar(&o.City, "city", "", "city")
	cmd.Flags().StringVar(&o.State, "state", "", "state abbreviation")
	cmd.Flags().StringVar(&o.PostalCode, "postalCode", "", "postal code")
	cmd.Flags().StringVar(&o.CountryCode, "countryCode", "", "country code (e.g. US)")
	cmd.Flags().IntVar(&o.Limit, "limit", 0, "max results to return")
	cmd.Flags().IntVar(&o.Skip, "skip", 0, "number of results to skip")

	return cmd
}

func newNPIColCmd(cfg *core.Config) *cobra.Command {
	var summary bool

	cmd := &cobra.Command{
		Use:   "col <column> [files...]",
		Short: "Validate NPI values in a CSV column",
		Long: `Read a column from CSV files and validate each value as an NPI number.

By default, prints each unique value with its frequency and validity.
Use --summary to print only the percentage of valid NPIs.

Examples:
  dkit npi col "NPI" *.csv
  dkit npi col --summary "Provider NPI" *.csv`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			col := args[0]
			list, err := resolveFiles(args[1:], cfg)
			if err != nil {
				return err
			}

			invalid, err := ops.NPIColValidate(list, ops.NPIColOpts{
				Column:  col,
				Summary: summary,
				Config:  cfg,
			})
			if err != nil {
				return err
			}
			if invalid > 0 {
				os.Exit(2)
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&summary, "summary", false, "print only valid/invalid percentage")

	return cmd
}
