package cli

import (
	"fmt"
	"os"

	"github.com/c-a-ray/dkit/internal/core"
	"github.com/c-a-ray/dkit/internal/ops"
	"github.com/spf13/cobra"
)

func addFilesCmd(root *cobra.Command, cfg *core.Config) {
	files := &cobra.Command{
		Use:   "files",
		Short: "File-level helpers",
	}

	files.AddCommand(newFilesWithCmd(cfg))

	root.AddCommand(files)
}

func newFilesWithCmd(cfg *core.Config) *cobra.Command {
	var ci bool
	cmd := &cobra.Command{
		Use:   "with <COL> <VALUE> [files...]",
		Short: "List files where column equals VALUE",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			col := args[0]
			val := args[1]
			files := args[2:]
			if len(files) == 0 {
				return fmt.Errorf("no files")
			}
			list, err := core.ExpandFiles(files)
			if err != nil {
				return err
			}
			n, err := ops.FilesWith(list, ops.FilesWithOpts{
				Column:          col,
				Value:           val,
				CaseInsensitive: ci,
				Config:          cfg,
			})
			if err != nil {
				return err
			}
			if n == 0 {
				os.Exit(2)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&ci, "case-insensitive", false, "case-insensitive match")
	return cmd
}
