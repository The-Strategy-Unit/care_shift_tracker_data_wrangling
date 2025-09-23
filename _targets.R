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
  tar_target(next_month, Sys.Date() |>
               lubridate::ceiling_date("month")),
  
  # Lookups --------------------------------------------------------------------
  ## Sub-geographies (LSOA, GP) to higher geographies (ICB, LA, PCN) -----------
  tar_target(
    lsoa_to_higher_geographies,
    sf::st_read(
      "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA21_SICBL24_ICB24_CAL24_LAD24_EN_LU/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
    ) |>
      janitor::clean_names() |>
      dplyr::select(-geometry)
  ),
  tar_target(
    gp_to_pcn,
    read.csv("data/epcncorepartnerdetails (Include headers).csv") |>
      janitor::clean_names() |>
      # Exclude GPs in Scotland and Wales:
      dplyr::filter(
        !stringr::str_starts(partner_organisation_code, "W"),
        !stringr::str_starts(partner_organisation_code, "S")
      ) |>
      # To get the latest GP to PCN mapping:
      dplyr::mutate(
        month_end = practice_to_pcn_relationship_end_date |>
          stringr::str_replace_na(as.character(next_month)) |>
          lubridate::ymd()
      ) |>
      dplyr::filter(month_end == max(month_end), .by = partner_organisation_code)
  ),
  tar_target(
    lsoa11_to_lsoa_21,
    DBI::dbGetQuery(
      con, 
      "
      SELECT 
        lsoa11cd,
        lsoa21cd,
        chgind
    
      FROM [Internal_Reference].[DRD_LSOA11_LSOA21_UTLA_1]
      ") |>
      janitor::clean_names() |>
      unique()),
  
  # Population data ------------------------------------------------------------
  tar_target(
    population_lsoa,
    get_population_lsoa(age_cutoff, start_date, con)
  ),
  tar_target(
    population_lsoa_mapped_to_higher_geographies,
    population_lsoa |> 
      dplyr::rename(lsoa11cd = area_code) |>
      dplyr::left_join(lsoa11_to_lsoa_21, by = "lsoa11cd") |>
      dplyr::left_join(lsoa_to_higher_geographies |>
                         dplyr::select(-geometry),
                       by = "lsoa21cd") |>
      dplyr::mutate(number = dplyr::n(), 
                    .by = c(lsoa11cd, effective_snapshot_date),
                    population_size_amended = population_size / number)
  ),
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      population,
      get_higher_geography_population_from_lsoa(
        population_lsoa_mapped_to_higher_geographies, 
        geography)
    )
  ),
  targets::tar_target(
    latest_population_year,
    population_icb |>
      dplyr::summarise(population_year = max(as.numeric(population_year))) |>
      dplyr::pull(population_year)
  ),
  
  ## Geography codes to names --------------------------------------------------
  tar_target(
    icb_lookup,
    lsoa_to_higher_geographies |>
      dplyr::select(icb = icb24cdh, icb_name = icb24nm) |>
      unique()
  ),
  tar_target(
    la_lookup,
    lsoa_to_higher_geographies |>
      dplyr::select(la = lad24cd, la_name = lad24nm) |>
      unique()
  ),
  tar_target(
    pcn_lookup,
    gp_to_pcn |>
      dplyr::select(pcn = pcn_code, pcn_name) |>
      unique()
  ),
  
  # Elective to non elective admissions ratio ----------------------------------
  # ICB and LA
  tar_target(
    elective_non_elective_lsoa,
    get_elective_non_elective_admissions_sub_geography("lsoa", 
                                                       age_cutoff, 
                                                       start_date, 
                                                       con)  |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies)
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
      join_to_geography_lookup("pcn", gp_to_pcn) 
  ),
  tar_target(
    elective_non_elective_ratio_pcn,
    get_elective_non_elective_ratio(elective_non_elective_gp, "pcn")
  ),
  
  # Frailty
  tarchetypes::tar_file(
    frailty_risk_scores_filename,
    "data/frailty_risk_scores.csv"
  ),
  tar_target(
    frailty_risk_scores,
    read.csv(frailty_risk_scores_filename) |>
      janitor::clean_names()
  ),
  tarchetypes::tar_map(
    list(sub_geography = c("gp", "lsoa")),
    tar_target(
      frailty_beddays,
      get_frailty_beddays_sub_geography(sub_geography, 
                                        start_date, 
                                        con, 
                                        frailty_risk_scores)
    )
  ),
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      frailty_beddays,
      get_frailty_beddays_geography(frailty_beddays_lsoa, 
                             geography, 
                             lsoa_to_higher_geographies)
    )
  ),
  tar_target(
    frailty_beddays_pcn,
    get_frailty_beddays_geography(frailty_beddays_gp, 
                           "pcn", 
                           gp_to_pcn)
  ),
  tar_target(
    frailty_indicators_icb,
    get_frailty_indicators(frailty_beddays_icb,
                           population_icb,
                           "icb",
                           latest_population_year)
  ),
  tar_target(
    frailty_indicators_la,
    get_frailty_indicators(frailty_beddays_la,
                           population_la,
                           "la",
                           latest_population_year)
  )
  
)
