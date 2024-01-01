use clap::*;
use polars::prelude::*;
use std::env::args;
use std::ffi::OsStr;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::{env, error::Error};

enum FileFormat {
    Csv,
    Tsv,
    Parquet,
}

fn parse_args() -> ArgMatches {
    let args_match: ArgMatches = command!()
        .name(crate_name!())
        .about(crate_description!())
        .version(crate_version!())
        .author(crate_authors!("\n"))
        .arg(
            Arg::new("filepath")
                .help("The path to the file")
                .required(true),
        )
        .arg(
            Arg::new("n_rows")
                .short('n')
                .long("num-rows")
                .help("Number of rows to print")
                .required(false)
                .default_missing_value("10")
                .value_parser(clap::value_parser!(usize)),
        )
        .arg(
            Arg::new("select_columns")
                .short('s')
                .long("select")
                .help("Columns to display")
                .required(false),
        )
        .arg(
            Arg::new("delimiter")
                .short('d')
                .long("delimiter")
                .help("Character used to separate columns")
                .required(false),
        )
        .arg(
            Arg::new("head")
                .long("head")
                .help("Print only the first n rows")
                .action(ArgAction::SetTrue)
                .conflicts_with_all(["sample", "tail"]),
        )
        .arg(
            Arg::new("tail")
                .long("tail")
                .help("Print only the last n rows")
                .action(ArgAction::SetTrue)
                .conflicts_with_all(["head", "sample"]),
        )
        .arg(
            Arg::new("sample")
                .long("sample")
                .help("Print only a random subset of n rows")
                .action(ArgAction::SetTrue)
                .conflicts_with_all(["head", "tail"]),
        )
        .arg(
            Arg::new("no_header")
                .long("no-header")
                .help("Table has no header row")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("describe")
                .short('D')
                .long("describe")
                .help("Print summary statistics")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("column_names")
                .short('c')
                .long("column-names")
                .help("Get column names")
                .action(ArgAction::SetTrue)
                .conflicts_with_all([
                    "head",
                    "tail",
                    "sample",
                    "n_rows",
                    "select_columns",
                    "no_header",
                    "describe",
                ]),
        )
        .get_matches();

    args_match
}

// get extension from filepath
// adapted from https://stackoverflow.com/a/45292067
fn get_format_from_filename(filename: &str) -> FileFormat {
    let file_extension = Path::new(filename).extension().and_then(OsStr::to_str);
    match file_extension {
        Some("csv") => FileFormat::Csv,
        Some("tsv") => FileFormat::Tsv,
        Some("parquet") => FileFormat::Parquet,
        _ => {
            panic!("File extension not recognized. Valid file extensions are: .csv, .tsv, .parquet")
        }
    }
}

// determine delimiter based on file extension
fn get_default_delimiter(format: &FileFormat) -> char {
    match format {
        FileFormat::Csv => ',',
        FileFormat::Tsv => '\t',
        _ => panic!("Unsupported format"),
    }
}

// get the number of rows to parse
fn get_num_rows_to_parse(args_matches: &ArgMatches) -> Option<usize> {
    let want_random_samples = *args_matches
        .get_one::<bool>("sample")
        .expect("Unexpected value for sample");

    let want_tail: bool = *args_matches
        .get_one::<bool>("tail")
        .expect("Unexpected value for tail");

    let describe_table: bool = *args_matches
        .get_one::<bool>("describe")
        .expect("Unexpected value for describe");

    if want_random_samples || want_tail || describe_table {
        return None;
    }

    if let Some(n_rows) = args_matches.get_one::<usize>("n_rows") {
        return Some(*n_rows);
    }

    let want_header_rows = *args_matches
        .get_one::<bool>("head")
        .expect("Unexpected value for head");

    if want_header_rows {
        return Some(10);
    }

    None
}

// get columns to display
fn get_columns_to_select(args_matches: &ArgMatches) -> Option<Vec<String>> {
    if let Some(column_selection) = args_matches.get_one::<String>("select_columns") {
        return Some(column_selection.split(',').map(|s| s.to_string()).collect());
    }

    None
}

// has header row?
fn has_header_row(args_matches: &ArgMatches) -> bool {
    !*args_matches
        .get_one::<bool>("no_header")
        .expect("Unexpected value for no_header")
}

// get delimiter to use
fn get_delimiter(args_matches: &ArgMatches) -> Option<char> {
    if let Some(delimiter) = args_matches.get_one::<char>("delimiter") {
        return Some(*delimiter);
    }

    let filepath = args_matches.get_one::<String>("filepath").unwrap();
    let file_format = get_format_from_filename(filepath);
    let delimiter = get_default_delimiter(&file_format);

    Some(delimiter)
}

// parse csv and tsv files
fn parse_csv_file(
    filepath: &str,
    select_columns: Option<Vec<String>>,
    n_rows: Option<usize>,
    delimiter: Option<char>,
    has_header: bool,
) -> DataFrame {
    let delimiter = delimiter.unwrap_or(',');

    CsvReader::from_path(filepath)
        .expect(&format!("Unable to parse the file {}", filepath))
        .with_separator(delimiter as u8)
        .has_header(has_header)
        .with_columns(select_columns)
        .with_n_rows(n_rows)
        .finish()
        .expect(&format!("Unable to parse the file {}", filepath))
}

// parse parquet file
fn parse_parquet_file(
    filepath: &str,
    select_columns: Option<Vec<String>>,
    n_rows: Option<usize>,
) -> DataFrame {
    let f = File::open(filepath).expect(&format!("Unable to open the file {}", filepath));

    ParquetReader::new(f)
        .with_columns(select_columns)
        .with_n_rows(n_rows)
        .finish()
        .expect(&format!("Unable to parse the Parquet file {}", filepath))
}

// get column names in dataframe
// fn get_column_names(df: &DataFrame) -> Result<Vec<String>, E> {}

/// Configure Polars with ENV vars
pub fn configure_the_environment() {
    env::set_var("POLARS_FMT_TABLE_DATAFRAME_SHAPE_BELOW", "1"); // print shape information below the table.
    env::set_var("POLARS_FMT_TABLE_FORMATTING", "ASCII_MARKDOWN"); // define styling of tables using any of the following options.

    env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1"); // apply rounded corners to UTF8-styled tables.
    env::set_var("POLARS_FMT_MAX_COLS", "12"); // maximum number of columns shown when formatting DataFrames.
    env::set_var("POLARS_FMT_MAX_ROWS", "-1"); // maximum number of rows shown when formatting DataFrames.
}

fn main() -> () {
    let args_match: ArgMatches = parse_args();

    configure_the_environment();

    let filepath = args_match
        .get_one::<String>("filepath")
        .expect("Filepath is required");

    if !PathBuf::from(filepath).is_file() {
        panic!("File not found at {}", filepath);
    }

    let file_format = get_format_from_filename(filepath);
    let select_columns = get_columns_to_select(&args_match);
    let n_rows = get_num_rows_to_parse(&args_match);

    let delimiter = get_delimiter(&args_match);
    let has_header = has_header_row(&args_match);

    let df = match file_format {
        FileFormat::Parquet => parse_parquet_file(filepath, select_columns, n_rows),
        _ => parse_csv_file(filepath, select_columns, n_rows, delimiter, has_header),
    };

    // describe the table
    let describe_table: bool = *args_match
        .get_one::<bool>("describe")
        .expect("Unexpected value for describe");
    if describe_table {
        println!(
            "{}",
            df.describe(None).expect("Unable to get summary statistics")
        );
    }

    // print tail

    let want_tail: bool = *args_match
        .get_one::<bool>("tail")
        .expect("Unexpected value for tail");
    if want_tail {
        println!("{}", df.tail(None));
    }

    // print sample
    let want_random_samples = *args_match
        .get_one::<bool>("sample")
        .expect("Unexpected value for sample");
    if want_random_samples {
        let sample_size = *args_match
            .get_one::<usize>("n_rows")
            .unwrap_or(&df.height());
        println!("{}", sample_size);
        println!(
            "{}",
            df.sample_n_literal(sample_size, false, false, None)
                .expect("Unable to get summary statistics")
        );
    }

    // print entire df
    // println!("{}", df);
}
