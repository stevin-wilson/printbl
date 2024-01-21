use clap::*;
use clap::{Arg, ArgAction, Command};
use polars::io::mmap::MmapBytesReader;
use polars::prelude::*;
use std::env;
use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

// --------------------------------------------------
enum FileFormat {
    Csv,
    Tsv,
    Parquet,
}

// --------------------------------------------------
#[derive(Debug)]
struct CliArgs {
    filepath: String,
    max_rows: Option<u32>,
    delimiter: Option<char>,
    selected_columns: Option<Vec<String>>,
    no_header: bool,
    column_names_only: bool,
    describe: bool,
    head: bool,
    tail: bool,
    sample: bool,
    markdown: bool,
}

// --------------------------------------------------
fn get_args() -> CliArgs {
    let args_match: ArgMatches = Command::new(crate_name!())
        .about(crate_description!())
        .version(crate_version!())
        .author(crate_authors!("\n"))
        .arg(
            Arg::new("filepath")
                .help("The path to the file")
                .required(false)
                .default_value("-"),
        )
        .arg(
            Arg::new("max_rows")
                .short('n')
                .long("max-rows")
                .value_name("MAX-ROWS")
                .help("Number of rows to print")
                .required(false)
                //.default_missing_value("10")
                .value_parser(clap::value_parser!(u32).range(1..)),
        )
        .arg(
            Arg::new("delimiter")
                .short('d')
                .long("delimiter")
                .help("Character used to separate columns")
                .required(false),
        )
        .arg(
            Arg::new("select_columns")
                .short('s')
                .long("select")
                .help("Columns to display")
                .required(false),
        )
        .arg(
            Arg::new("no_header")
                .long("no-header")
                .help("Table has no header row")
                .action(ArgAction::SetTrue),
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
            Arg::new("describe")
                .short('D')
                .long("describe")
                .help("Print summary statistics")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("markdown")
                .short('m')
                .long("markdown")
                .help("Format print for markdown documents")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("column_names_only")
                .short('c')
                .long("column-names-only")
                .help("Get column names")
                .action(ArgAction::SetTrue)
                .conflicts_with_all([
                    "head",
                    "tail",
                    "sample",
                    "max_rows",
                    "select_columns",
                    "no_header",
                    "describe",
                    "markdown",
                ]),
        )
        .get_matches();

    CliArgs {
        filepath: args_match
            .get_one::<String>("filepath")
            .expect("Filepath is required")
            .clone(),
        max_rows: args_match.get_one::<u32>("max_rows").copied(),
        delimiter: args_match.get_one::<char>("delimiter").copied(),
        selected_columns: args_match
            .get_one::<String>("select_columns")
            .map(|s| s.split(',').map(String::from).collect()),
        no_header: args_match.get_flag("no_header"),
        column_names_only: args_match.get_flag("column_names_only"),
        describe: args_match.get_flag("describe"),
        head: args_match.get_flag("head"),
        tail: args_match.get_flag("tail"),
        sample: args_match.get_flag("sample"),
        markdown: args_match.get_flag("markdown"),
    }
}

// --------------------------------------------------
// get extension from filepath
// adapted from https://stackoverflow.com/a/45292067
fn get_format_from_filename(filename: &str) -> Option<&FileFormat> {
    let file_extension = Path::new(filename).extension().and_then(OsStr::to_str);
    match file_extension {
        Some("csv") => Some(&FileFormat::Csv),
        Some("tsv") => Some(&FileFormat::Tsv),
        Some("parquet") => Some(&FileFormat::Parquet),
        _ => None,
    }
}

// --------------------------------------------------
// determine delimiter based on file extension
fn get_default_delimiter(format: Option<&FileFormat>) -> char {
    match format {
        Some(&FileFormat::Tsv) => '\t',
        _ => ',',
    }
}

// --------------------------------------------------
// filepath: String,
// max_rows: Option<usize>,
// delimiter: Option<char>,
// selected_columns: Option<Vec<String>>,
// no_header: bool,
// fn parse_table(
//     filepath: &str,
//     max_rows: Option<usize>,
//     delimiter: Option<char>,
//     selected_columns: Option<Vec<String>>,
//     no_header: bool,
// ) -> DataFrame {
//     // if filepath is -, read from stdin
//     if filepath == "-" {
//         return parse_from_stdin(
//             selected_columns,
//             max_rows,
//             delimiter.unwrap_or(','),
//             !no_header,
//         );
//     }
//     // if parquet file, parse it from filepath
//     else if filepath.ends_with(".parquet") {
//         return parse_parquet_file(filepath, selected_columns, max_rows);
//     } else {
//         let delimiter = get_delimiter(filepath, delimiter);
//         return parse_csv_file(filepath, selected_columns, max_rows, delimiter);
//     }
// }

// --------------------------------------------------
// get the number of rows to parse
fn get_num_rows_to_parse(
    max_rows: Option<u32>,
    head: bool,
    tail: bool,
    sample: bool,
    column_names_only: bool,
) -> Option<usize> {
    if column_names_only {
        return Some(1);
    }

    if tail || sample {
        return None;
    }

    if let Some(n_rows) = max_rows {
        return Some(n_rows as usize);
    }

    if head {
        return Some(10);
    }

    None
}

// --------------------------------------------------
// get delimiter to use
fn get_delimiter(file_format: Option<&FileFormat>, delimiter: Option<char>) -> char {
    if let Some(character) = delimiter {
        return character;
    }

    let delimiter = get_default_delimiter(file_format);

    delimiter
}

// --------------------------------------------------
// adapted from https://stackoverflow.com/a/77156312/11392276
fn parse_from_stdin(
    select_columns: Option<Vec<String>>,
    n_rows: Option<usize>,
    delimiter: char,
    has_header: bool,
) -> DataFrame {
    let mut v = Vec::<u8>::new();
    let reader = std::io::stdin()
        .lock()
        .read_to_end(&mut v)
        .expect("cannot read from stdin");

    let cursor = std::io::Cursor::new(v);
    let file = Box::new(cursor) as Box<dyn MmapBytesReader>;

    CsvReader::new(file)
        .with_separator(delimiter as u8)
        .has_header(has_header)
        .with_columns(select_columns)
        .with_n_rows(n_rows)
        .finish()
        .expect("Unable to parse table from stdin")
}

// --------------------------------------------------
// parse csv and tsv files
fn parse_csv_file(
    filepath: &str,
    select_columns: Option<Vec<String>>,
    n_rows: Option<usize>,
    delimiter: char,
    has_header: bool,
) -> DataFrame {
    CsvReader::from_path(filepath)
        .expect(&format!("Unable to parse the file {}", filepath))
        .with_separator(delimiter as u8)
        .has_header(has_header)
        .with_columns(select_columns)
        .with_n_rows(n_rows)
        .finish()
        .expect(&format!("Unable to parse the file {}", filepath))
}

// --------------------------------------------------
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

// --------------------------------------------------
// get column names in dataframe
fn get_column_names(df: DataFrame) -> Vec<String> {
    df.get_column_names()
        .into_iter()
        .map(String::from)
        .collect()
}

// --------------------------------------------------
/// Configure Polars with ENV vars
fn configure_the_environment(for_markdown: &bool) {
    env::set_var("POLARS_FMT_TABLE_DATAFRAME_SHAPE_BELOW", "1"); // print shape information below the table.
    env::set_var("POLARS_FMT_MAX_ROWS", "-1"); // maximum number of rows shown when formatting DataFrames.
    if *for_markdown {
        env::set_var("POLARS_FMT_TABLE_FORMATTING", "ASCII_MARKDOWN"); // define styling of tables using any of the following options.
        env::set_var("POLARS_FMT_MAX_COLS", "-1"); // maximum number of columns shown when formatting DataFrames.
    } else {
        env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1"); // apply rounded corners to UTF8-styled tables.
    }
}

// --------------------------------------------------
fn main() -> () {
    let cli_args: CliArgs = get_args();

    configure_the_environment(&cli_args.markdown);

    let n_rows = get_num_rows_to_parse(
        cli_args.max_rows,
        cli_args.head,
        cli_args.tail,
        cli_args.sample,
        cli_args.column_names_only,
    );

    let file_format = get_format_from_filename(&cli_args.filepath);
    let delimiter = get_delimiter(file_format, cli_args.delimiter);

    let df = {
        println!("{}", cli_args.filepath == String::from("-"));
        if cli_args.filepath == String::from("-") {
            parse_from_stdin(
                cli_args.selected_columns,
                n_rows,
                delimiter,
                !cli_args.no_header,
            )
        } else {
            if !PathBuf::from(cli_args.filepath.clone()).is_file() {
                panic!("File not found at {}", cli_args.filepath);
            }

            match file_format {
                Some(&FileFormat::Parquet) => {
                    parse_parquet_file(&cli_args.filepath, cli_args.selected_columns, n_rows)
                }
                None => panic!(),
                _ => parse_csv_file(
                    &cli_args.filepath,
                    cli_args.selected_columns,
                    n_rows,
                    delimiter,
                    !cli_args.no_header,
                ),
            }
        }
    };

    // print column names
    if cli_args.column_names_only {
        println!("{:#?}", get_column_names(df.clone()))
    }

    // describe the table
    if cli_args.describe {
        println!(
            "{}",
            df.describe(None).expect("Unable to get summary statistics")
        );
    }

    // print tail
    if cli_args.tail {
        println!("{}", df.tail(None));
    }

    // print sample
    if cli_args.sample {
        let sample_size = {
            if let Some(s_size) = cli_args.max_rows {
                s_size as usize
            } else {
                df.height()
            }
        };
        println!("{}", sample_size);
        println!(
            "{}",
            df.sample_n_literal(sample_size, false, false, None)
                .expect("Unable to get summary statistics")
        );
    }

    // print entire df
    println!("{}", df);
}
