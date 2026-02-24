# dkit

A CLI toolkit for exploring, analyzing, and transforming tabular data. Supports CSV, TSV, pipe-delimited, and fixed-width files.

## Install

```bash
go install github.com/c-a-ray/dkit/cmd/dkit@latest
```

Or build from source:

```bash
make build    # compiles to bin/dkit
```

## Commands

### `dkit col` — Column operations

#### `col cmp` — Compare columns

Compare column A against one or more target columns. A row matches if A equals ANY target (OR logic).

```bash
dkit col cmp ID ID2 *.csv
dkit col cmp Name First,Last *.csv          # Name must equal First OR Last
dkit col cmp ID ID2 --ignoreCase *.csv
dkit col cmp ID ID2 --to table *.csv        # formatted table output
dkit col cmp ID ID2 --to 'csv out.csv' *.csv
```

| Flag | Description |
|------|-------------|
| `--ignoreCase` | Case-insensitive comparison |
| `--allowEmpty` | Compare even if one/both columns are empty |
| `--to` | Output format: `lines` (default), `table`, or `csv /path/to/file.csv` |

Exit code 2 if mismatches found.

#### `col vals` — Unique values or frequency counts

```bash
dkit col vals uniq City *.csv
dkit col vals freq Status *.csv
dkit col vals freq City --when "Status=active" *.csv
dkit col vals freq Name --fixedWidth "0:10" data.txt
```

| Flag | Description |
|------|-------------|
| `--when`, `-w` | Filter rows by condition (repeatable; see [Row filtering](#row-filtering)) |
| `--nullToken` | Token for empty cells in freq mode (default: `<EMPTY>`) |
| `--fixedWidth` | Extract fixed-width column `START:END` (0-based, exclusive end) |

#### `col first` — First non-empty value per file

```bash
dkit col first Name *.csv
```

Exit code 2 if no non-empty values found.

#### `col dupkey` — Duplicate key detection

Report keys that map to multiple distinct tuples. Useful for finding conflicting data.

```bash
dkit col dupkey PatientID --by "SSN,DOB" *.csv
dkit col dupkey Status --by "First Name,Last Name" --ignoreCase *.csv
```

| Flag | Description |
|------|-------------|
| `--by` | **(required)** Comma-separated columns forming the tuple |
| `--ignoreCase` | Case-insensitive comparisons |
| `--requireAll` | Skip rows where any BY field is empty |
| `--nullToken` | Token for empty BY fields (default: `<EMPTY>`) |

Exit code 2 if conflicting keys found.

#### `col list` — List column names

List unique column names across files.

```bash
dkit col list *.csv
dkit col list --sorted *.csv
dkit col list --oneline --outdelim tab *.csv
```

| Flag | Description |
|------|-------------|
| `--sorted` | Sort alphabetically |
| `--oneline` | Print all columns on one line |
| `--outdelim`, `-o` | Output delimiter for `--oneline` (default: `comma`) |

---

### `dkit files` — File-level helpers

#### `files with` — Filter files by column value

```bash
dkit files with City NYC *.csv
dkit files with Status active --caseInsensitive *.csv
```

| Flag | Description |
|------|-------------|
| `--caseInsensitive` | Case-insensitive match |

Exit code 2 if no matching files found.

---

### `dkit fmt` — Format conversion

Rewrite files with different delimiters.

```bash
dkit fmt --inDelim tab --outDelim pipe -o output.psv input.tsv
dkit fmt --inDelim comma --outDelim tab --outdir converted/ *.csv
dkit fmt --inDelim comma --outDelim tab --inplace *.csv
```

| Flag | Description |
|------|-------------|
| `--inDelim` | **(required)** Input delimiter |
| `--outDelim` | **(required)** Output delimiter |
| `--out`, `-o` | Write to single output file (one input only) |
| `--outdir` | Write each input to this directory |
| `--ext` | Output extension with `--outdir` (auto: `.csv` if comma, `.txt` otherwise) |
| `--inplace`, `-i` | Rewrite files in place |

---

### `dkit cmp` — Compare archives

#### `cmp zips` — Compare ZIP archives

```bash
dkit cmp zips archive1.zip archive2.zip
dkit cmp zips archive1.zip archive2.zip --summaryOnly
```

| Flag | Description |
|------|-------------|
| `--summaryOnly`, `-s` | Show only summary, skip detailed diffs |
| `--ignoreMissing` | Don't error if files are missing from one archive |

Exit code 2 if archives differ.

---

### `dkit npi` — NPI registry operations

#### `npi valid` — Validate NPI number

```bash
dkit npi valid 1164905659
```

Exit code 2 if invalid.

#### `npi lookup` — Search NPI registry

Query the CMS NPI registry by any combination of parameters.

```bash
dkit npi lookup --number 1164905659
dkit npi lookup --firstName John --lastName Smith --state NY
dkit npi lookup --organizationName "Mayo Clinic" --state MN
```

| Flag | Description |
|------|-------------|
| `--number` | NPI number |
| `--enumerationType` | `NPI-1` (individual) or `NPI-2` (organization) |
| `--firstName` | Provider first name |
| `--lastName` | Provider last name |
| `--organizationName` | Organization name |
| `--city` | City |
| `--state` | State abbreviation |
| `--postalCode` | Postal code |
| `--countryCode` | Country code (e.g., `US`) |
| `--taxonomyDescription` | Taxonomy description |
| `--useFirstNameAlias` | Include first name aliases |
| `--addressPurpose` | `LOCATION` or `MAILING` |
| `--limit` | Max results |
| `--skip` | Number of results to skip |

Exit code 2 if no results found.

#### `npi col` — Validate NPI column in files

```bash
dkit npi col "NPI" *.csv
dkit npi col --summary "Provider NPI" *.csv
```

| Flag | Description |
|------|-------------|
| `--summary` | Print only valid/invalid percentage |

Exit code 2 if any invalid NPIs found.

---

## Global Flags

These flags apply to all commands and can also be set via [config file](#config-file).

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--delim` | `-d` | `,` | Field delimiter (single char, or: `tab`, `comma`, `pipe`, `space`) |
| `--encoding` | `-e` | `utf-8-sig` | Input encoding (`utf-8`, `utf-8-sig`, `iso-8859-1`, `latin1`) |
| `--noHeader` | `-H` | `false` | Treat first row as data, use numeric column indexes |
| `--quiet` | `-q` | `false` | Suppress per-row output |
| `--lazyQuotes` | | `false` | Allow bare quotes inside unquoted fields |
| `--skipStart` | | `0` | Lines to skip at start of each file |
| `--skipEnd` | | `0` | Lines to skip at end of each file |
| `--fieldsPerRecord` | | | Expected fields per record (`variable` for any count, or an integer) |

## Row Filtering

The `--when` flag on `col vals` filters rows using CNF (conjunctive normal form):

- Multiple `--when` flags are **ANDed** (all must match)
- Pipe `|` within a flag creates **OR** groups (any can match)

### Condition types

| Syntax | Meaning |
|--------|---------|
| `COL=VAL` | Exact equality |
| `COL!=VAL` | Not equal |
| `COL is empty` | Column is blank |
| `COL not empty` | Column is not blank |

### Examples

```bash
# Single condition
dkit col vals freq Status --when "City=NYC" *.csv

# AND: both must match
dkit col vals freq Name --when "City=NYC" --when "Status=active" *.csv

# OR within a group
dkit col vals freq Name --when "City=NYC|City=LA" *.csv

# Combined: (City=NYC OR City=LA) AND Status=active
dkit col vals freq Name --when "City=NYC|City=LA" --when "Status=active" *.csv
```

## Config File

Instead of passing the same flags every time, create a YAML config file for your project directory.

### Setup

```bash
# Point current directory at a config file
dkit config set ./dkit.yaml

# Show active config
dkit config show

# Remove config from current directory
dkit config reset
```

`config set` creates a `.dkit` dotfile in the current directory containing the path to your YAML config. All subsequent dkit commands in that directory will load the config automatically.

### Config format

```yaml
# File globs — used when no files are passed on the command line
files:
  - data/*.csv
  - reports/*.txt

# Delimiter (single char or: tab, comma, pipe, space)
delimiter: pipe

# Input encoding
encoding: utf-8-sig

# Skip junk header/footer lines
skipStart: 1
skipEnd: 1

# Allow variable number of columns
fieldsPerRecord: variable

# Other options
noHeader: false
quiet: false
lazyQuotes: true

# Fixed-width column definitions (see below)
fixedColumns:
  - name: FirstName
    start: 0
    end: 10
  - name: LastName
    start: 10
    end: 20
  - name: City
    start: 20
    end: 30
```

### Precedence

CLI flags always win:

```
built-in defaults  →  config file  →  CLI flags
```

Only explicitly-passed CLI flags override config values. Flags you don't set on the command line are left at whatever the config file specified.

```bash
# Config says skipEnd: 1, but this overrides it to 0
dkit col vals uniq Name --skipEnd 0
```

### Using file globs from config

When `files` is set in the config, you can omit file arguments entirely:

```bash
# Without config — must specify files
dkit col list *.csv

# With config (files: ["data/*.csv"]) — files come from config
dkit col list
```

Files passed on the command line always take priority over the config's `files` list.

## Fixed-Width Files

dkit can parse fixed-width (columnar) files by defining column positions in the config file. Indexes are 0-based with exclusive end (Python-style slicing).

### Config

```yaml
fixedColumns:
  - name: FirstName
    start: 0
    end: 10
  - name: LastName
    start: 10
    end: 20
  - name: City
    start: 20
    end: 30
  - name: Status
    start: 30
    end: 40
```

When `fixedColumns` is defined, dkit treats each line as a fixed-width record instead of parsing CSV delimiters. Column names from the config become the header, so you can reference them by name:

```bash
dkit col vals uniq FirstName
dkit col list
dkit files with City Houston
```

Use `--skipStart` / `--skipEnd` (or config equivalents) to skip any non-data header/footer lines in the file.

### Single-column extraction

For one-off fixed-width extraction without a config file, use `--fixedWidth` on `col vals`:

```bash
dkit col vals freq Name --fixedWidth "0:10" data.txt
```

## Examples

### Data quality checks

```bash
# Value distribution
dkit col vals freq Status *.csv

# Unique values
dkit col vals uniq "Patient ID" *.csv

# Find conflicting records
dkit col dupkey "Patient ID" --by "SSN,DOB" *.csv

# Compare columns across files
dkit col cmp ID ID2 -q *.csv
```

### Filter and analyze

```bash
# Frequency of cities for active records
dkit col vals freq City --when "Status=active" *.csv

# Which files contain active records?
dkit files with Status active *.csv
```

### Convert formats

```bash
# CSV to pipe-delimited
dkit fmt --inDelim comma --outDelim pipe --outdir output/ *.csv

# Tab to CSV, in place
dkit fmt --inDelim tab --outDelim comma --inplace *.tsv
```

### Work with messy files

```bash
# Skip 1 header line and 1 footer line, pipe-delimited
dkit col vals uniq City --skipStart 1 --skipEnd 1 -d pipe *.txt
```

### Validate NPI data

```bash
dkit npi col "Provider NPI" *.csv
dkit npi lookup --firstName John --lastName Smith --state NY
```

### Project config for repeated use

```yaml
# dkit.yaml
files:
  - reports/*.txt
delimiter: pipe
skipStart: 1
skipEnd: 1
```

```bash
dkit config set dkit.yaml
dkit col list                    # uses config for files, delimiter, skip
dkit col vals freq Status        # same — no flags needed
dkit config reset                # done with this project
```
