use clap::{ArgAction, Args, Parser};

#[derive(Debug, Parser)]
#[command(
    name = "db-rehearsal",
    version,
    about = "CLI for database rehearsal workflows",
    disable_help_flag = true,
    disable_version_flag = true
)]
struct Cli {
    #[command(flatten)]
    required: RequiredArgs,

    #[command(flatten)]
    options: OptionalArgs,
}

#[derive(Debug, Args)]
#[command(next_help_heading = "Required")]
struct RequiredArgs {
    /// The source version of the database
    #[arg(short, long)]
    source_version: String,

    /// The target version of the database
    #[arg(short, long)]
    target_version: String,
}

#[derive(Debug, Args)]
#[command(next_help_heading = "Options")]
struct OptionalArgs {
    /// Show full per-query details
    #[arg(short, long, default_value_t = false)]
    verbose: bool,

    #[arg(short = 'h', long = "help", action = ArgAction::Help, help = "Print help")]
    help: bool,

    #[arg(
        short = 'V',
        long = "version",
        action = ArgAction::Version,
        help = "Print version"
    )]
    version: bool,
}

fn main() {
    let cli = Cli::parse();

    println!(
        "Rehearsing database from {} to {}",
        cli.required.source_version, cli.required.target_version
    );
    println!("Verbose: {}", cli.options.verbose);
}
