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
  tar_target(age_cutoff, 65),
  tar_target(start_date, "2008-04-01"),
  
  # Lookups --------------------------------------------------------------------
  tar_target(
    icb_lookup,
    DBI::dbGetQuery(con, "SELECT * FROM [Internal_Reference].[CCGToICB_1]") |>
      janitor::clean_names() |>
      dplyr::select(icb_code, icb_name) |>
      unique()
  ),
  tar_target(la_lookup, 2),
  tar_target(pcn_lookup, 2),
  
   tar_target(
    lsoa_to_higher_geographies,
    read.csv("data/LSOA_(2021)_to_SICBL_to_ICB_to_Cancer_Alliances_to_LAD_(April_2024)_Lookup_in_EN.csv") |>
      janitor::clean_names()
  ),
  
  # Elective to non elective admissions ratio ----------------------------------
  tar_target(
    elective_non_elective_by_lsoa,
    get_elective_non_elective_admissions_lsoa(age_cutoff, start_date, con) |>
      dplyr::left_join(lsoa_to_higher_geographies, 
                       by = c("der_postcode_lsoa_2021_code" = "lsoa21cd"))
    ),
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      elective_non_elective_ratio,
      get_elective_non_elective_ratio(elective_non_elective_by_lsoa, geography)
    )
  )
)
