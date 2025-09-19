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

# ODBC Connection --------------------------------------------------------------
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
  tar_target(next_month, 
             Sys.Date() |> 
               lubridate::ceiling_date("month")),
  
  # Lookups --------------------------------------------------------------------
  ## Sub-geographies (LSOA, GP) to higher geographies (ICB, LA, PCN) -----------
  tar_target(
    lsoa_to_higher_geographies,
    read.csv(
      "data/LSOA_(2021)_to_SICBL_to_ICB_to_Cancer_Alliances_to_LAD_(April_2024)_Lookup_in_EN.csv"
    ) |>
      janitor::clean_names()
  ),
  tar_target(
    gp_to_pcn,
    read.csv("data/epcncorepartnerdetails (Include headers).csv") |>
      janitor::clean_names() |>
      # Exclude GPs in Scotland and Wales:
      dplyr::filter(
        !stringr::str_starts(partner_organisation_code, "W"),
        !stringr::str_starts(partner_organisation_code, "S")) |>
      # To get the latest GP to PCN mapping:
      dplyr::mutate(month_end = practice_to_pcn_relationship_end_date |>
                      stringr::str_replace_na(as.character(next_month)) |>
                      lubridate::ymd()) |>
      dplyr::filter(month_end == max(month_end),
                    .by = partner_organisation_code)
  ),
  
  ## Geography codes to names --------------------------------------------------
  tar_target(
    icb_lookup,
    DBI::dbGetQuery(con, "SELECT * FROM [Internal_Reference].[CCGToICB_1]") |>
      janitor::clean_names() |>
      dplyr::select(icb_code, icb_name) |>
      unique()
  ),
  tar_target(la_lookup, 2),
  tar_target(pcn_lookup,
             gp_to_pcn |>
               dplyr::select(pcn_code, pcn_name) |>
               unique()),
  
  # Elective to non elective admissions ratio ----------------------------------
  # ICB and LA
  tar_target(
    elective_non_elective_lsoa,
    get_elective_non_elective_admissions_sub_geography("lsoa",
                                                       age_cutoff, 
                                                       start_date, 
                                                       con) |>
      dplyr::left_join(
        lsoa_to_higher_geographies,
        by = c("der_postcode_lsoa_2021_code" = "lsoa21cd")
      )
    ),
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      elective_non_elective_ratio,
      get_elective_non_elective_ratio(elective_non_elective_lsoa, geography)
    )
  ),
  # PCN
  tar_target(
    elective_non_elective_gp,
    get_elective_non_elective_admissions_sub_geography("gp",
                                                       age_cutoff, 
                                                       start_date, 
                                                       con) |>
      dplyr::left_join(gp_to_pcn,
                       by = c("gp_practice_sus" = "partner_organisation_code"))
    ),
  tar_target(
    elective_non_elective_ratio_pcn,
    get_elective_non_elective_ratio(elective_non_elective_gp, "pcn")
  )
)
