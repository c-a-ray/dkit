package main

import (
	"fmt"
	"os"

	"github.com/c-a-ray/dkit/internal/cli"
	"github.com/c-a-ray/dkit/internal/core"
)

func main() {
	cfg := core.NewConfig()
	root := cli.NewRootCmd(cfg)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
