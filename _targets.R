# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
library(targets)
# library(tarchetypes) # Load other packages as needed.

# Set target options:
tar_option_set(
  packages = c("tibble") # Packages that your targets need for their tasks.
  
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source()
# tar_source("other_functions.R") # Source other scripts as needed.


server <- "udalsyndataprod.sql.azuresynapse.net"
database <- "UDAL_Warehouse"

con <- DBI::dbConnect(
  odbc::odbc(),
  Driver = "ODBC Driver 17 for SQL Server",
  Server = server,
  Database = database,
  Authentication = "ActiveDirectoryInteractive"
)

# Replace the target list below with your own:
list(
  # Lookups --------------------------------------------------------------------
  tar_target(icb_lookup, 2),
  tar_target(la_lookup, 2),
  tar_target(pcn_lookup, 2),
  
  # Elective to non elective admissions ratio ----------------------------------
  tar_target(
    elective_non_elective_ratio_icb,
    get_elective_non_elective_ratio("Der_Postcode_CCG_Code")
  ),
  tar_target(
    elective_non_elective_ratio_la,
    get_elective_non_elective_ratio("Der_Postcode_Local_Auth")
  )
)
