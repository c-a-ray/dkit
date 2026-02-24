package core

import (
	"fmt"
	"strconv"

	"github.com/spf13/pflag"
)

// Config holds runtime options for reading and processing tabular data
type Config struct {
	Delim      rune
	Encoding   string
	NoHeader   bool
	Quiet      bool
	LazyQuotes     bool
	SkipStart      int
	SkipEnd        int
	FieldsPerRecord int
}

// NewConfig returns a Config initialized with default values
func NewConfig() *Config {
	return &Config{
		Delim:      ',',
		Encoding:   "utf-8-sig",
		NoHeader:   false,
		Quiet:      false,
		LazyQuotes: false,
	}
}

// FromFlags updates the Config fields from a parsed FlagSet
func (c *Config) FromFlags(fs *pflag.FlagSet) error {
	d, err := fs.GetString("delim")
	if err != nil {
		return err
	}

	dr, err := ParseDelim(d)
	if err != nil {
		return err
	}
	c.Delim = dr

	enc, err := fs.GetString("encoding")
	if err != nil {
		return err
	}
	c.Encoding = enc

	nh, err := fs.GetBool("noHeader")
	if err != nil {
		return err
	}
	c.NoHeader = nh

	q, err := fs.GetBool("quiet")
	if err != nil {
		return err
	}
	c.Quiet = q

	lq, err := fs.GetBool("lazyQuotes")
	if err != nil {
		return err
	}
	c.LazyQuotes = lq

	ss, err := fs.GetInt("skipStart")
	if err != nil {
		return err
	}
	c.SkipStart = ss

	se, err := fs.GetInt("skipEnd")
	if err != nil {
		return err
	}
	c.SkipEnd = se

	fpr, err := fs.GetString("fieldsPerRecord")
	if err != nil {
		return err
	}
	c.FieldsPerRecord, err = parseFieldsPerRecord(fpr)
	if err != nil {
		return err
	}

	return nil
}

func parseFieldsPerRecord(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	if s == "variable" {
		return -1, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("--fieldsPerRecord must be \"variable\" or an integer, got %q", s)
	}
	return n, nil
}

func ParseDelim(d string) (rune, error) {
	if d == "" || d == "," || d == "comma" {
		return ',', nil
	}
	if d == `\t` || d == "tab" {
		return '\t', nil
	}
	if d == "|" || d == "pipe" {
		return '|', nil
	}
	if d == " " || d == "space" {
		return ' ', nil
	}

	r := []rune(d)
	if len(r) != 1 {
		return 0, fmt.Errorf("--delim must be a single character or one of: tab, comma, pipe, space")
	}

	return r[0], nil
}
