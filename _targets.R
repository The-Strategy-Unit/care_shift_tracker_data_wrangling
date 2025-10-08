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
  tar_target(
    age_bands,
    "('65-69', '70-74', '75-79', '80-84', '85-89', '85+', '90-94', '95+')"),
  tar_target(
    age_bands_75_plus,
    "('75-79', '80-84', '85-89', '85+', '90-94', '95+')"),
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
          lubridate::ymd(),
        pcn_name = pcn_name |>
          stringr::str_to_title() |>
          stringr::str_replace_all("Pcn", "PCN")
      ) |>
      dplyr::filter(month_end == max(month_end), 
                    .by = partner_organisation_code)
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
      "
    ) |>
      janitor::clean_names() |>
      unique()
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
  
  # Population data ------------------------------------------------------------
  # LSOA to LA and ICB
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
                         dplyr::select(-geometry), by = "lsoa21cd") |>
      dplyr::mutate(
        number = dplyr::n(),
        .by = c(lsoa11cd, effective_snapshot_date),
        population_size_amended = population_size / number
      )
  ),
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      population,
      get_population_higher_geography_from_lsoa(
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
  
  # GP to PCN
  targets::tar_target(
    population_gp_pre_2017_04_01,
    get_population_gp_pre_2017_04_01(age_bands, start_date, con) 
  ),
  targets::tar_target(
    population_gp_post_2017_04_01,
    get_population_gp_post_2017_04_01(age_bands, start_date, con) 
  ),
  targets::tar_target(
    population_pcn,
    get_population_pcn(population_gp_pre_2017_04_01, 
                       population_gp_post_2017_04_01,
                       gp_to_pcn)
  ),
  ## Over 75 population --------------------------------------------------------
  # LSOA to LA and ICB (75+)
  tar_target(
    population_75_plus_lsoa,
    get_population_lsoa(75, start_date, con)
  ),
  tar_target(
    population_75_plus_lsoa_mapped_to_higher_geographies,
    population_75_plus_lsoa |>
      dplyr::rename(lsoa11cd = area_code) |>
      dplyr::left_join(lsoa11_to_lsoa_21, by = "lsoa11cd") |>
      dplyr::left_join(lsoa_to_higher_geographies |>
                         dplyr::select(-geometry), by = "lsoa21cd") |>
      dplyr::mutate(
        number = dplyr::n(),
        .by = c(lsoa11cd, effective_snapshot_date),
        population_size_amended = population_size / number
      )
  ),
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      population_75_plus,
      get_population_higher_geography_from_lsoa(
        population_lsoa_mapped_to_higher_geographies, 
        geography)
    )
  ),
  # GP to PCN (75+)
  targets::tar_target(
    population_75_plus_gp_pre_2017_04_01,
    get_population_gp_pre_2017_04_01(age_bands_75_plus, start_date, con) 
  ),
  targets::tar_target(
    population_75_plus_gp_post_2017_04_01,
    get_population_gp_post_2017_04_01(age_bands_75_plus, start_date, con) 
  ),
  targets::tar_target(
    population_75_plus_pcn,
    get_population_pcn(population_75_plus_gp_pre_2017_04_01, 
                       population_75_plus_gp_post_2017_04_01,
                       gp_to_pcn)
  ),
  
  # Indicators -----------------------------------------------------------------
  ## Elective to non elective admissions ratio ---------------------------------
  # LSOA and GP
  tar_target(
    elective_non_elective_lsoa,
    get_elective_non_elective_sub_geography("lsoa", 
                                            age_cutoff, 
                                            start_date, 
                                            con) |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies)
  ), 
  tar_target(
    elective_non_elective_gp,
    get_elective_non_elective_sub_geography("gp", 
                                            age_cutoff, 
                                            start_date, 
                                            con) |>
      join_to_geography_lookup("pcn", gp_to_pcn)
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(geography = rep(c("icb", "la"), 2),
         activity_type = rep(c("admissions", "beddays"), each = 2)),
    tar_target(
      elective_non_elective_ratio,
      get_elective_non_elective_ratio(elective_non_elective_lsoa, 
                                      geography, 
                                      activity_type)
    )
  ),
  # PCN
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      elective_non_elective_ratio_pcn,
      get_elective_non_elective_ratio(elective_non_elective_gp, 
                                      "pcn", 
                                      activity_type)
    )
  ),
  
  ## Older people with frailty admissions --------------------------------------
  tarchetypes::tar_file(
    frailty_risk_scores_filename,
    "data/frailty_risk_scores.csv"
  ),
  tar_target(
    frailty_risk_scores,
    read.csv(frailty_risk_scores_filename) |>
      janitor::clean_names()
  ),
  # LSOA and GP
  tarchetypes::tar_map(
    list(sub_geography = c("gp", "lsoa")),
    tar_target(
      frailty,
      get_frailty_sub_geography(sub_geography, 
                                start_date, 
                                con, 
                                frailty_risk_scores)
    )
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      frailty,
      get_frailty_geography(frailty_lsoa, 
                            geography, 
                            lsoa_to_higher_geographies)
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      frailty_indicators_icb,
      get_indicators_per_pop(
        frailty_icb,
        population_75_plus_icb,
        "icb",
        latest_population_year,
        activity_type
      )
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      frailty_indicators_la,
      get_indicators_per_pop(
        frailty_la,
        population_75_plus_la,
        "la",
        latest_population_year,
        activity_type
      )
    )
  ),
  # PCN
  tar_target(
    frailty_pcn,
    get_frailty_geography(frailty_gp, "pcn", gp_to_pcn)
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      frailty_indicators_pcn,
      get_indicators_per_pop(
        frailty_pcn,
        population_75_plus_pcn,
        "pcn",
        latest_population_year,
        activity_type
      )
    )
  ),
  
  ## Emergency readmission within 28 days --------------------------------------
  # LSOA and GP
  tar_target(
    readmission_within_28_days_lsoa,
    get_readmission_within_28_days_sub_geography("lsoa", 
                                                 age_cutoff, 
                                                 start_date, 
                                                 con) |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies)
  ),
  tar_target(
    readmission_within_28_days_gp,
    get_readmission_within_28_days_sub_geography("gp", 
                                                 age_cutoff, 
                                                 start_date, 
                                                 con) |>
      join_to_geography_lookup("pcn", gp_to_pcn)
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(geography = rep(c("icb", "la"), 2)),
    tar_target(
      readmission_within_28_days,
      get_readmission_within_28_days_geography(readmission_within_28_days_lsoa, 
                                               geography)
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      readmission_indicator_icb,
      get_indicators_per_pop(
        readmission_within_28_days_icb,
        population_icb,
        "icb",
        latest_population_year,
        activity_type
      )
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      readmission_indicator_la,
      get_indicators_per_pop(
        readmission_within_28_days_la,
        population_la,
        "la",
        latest_population_year,
        activity_type
      )
    )
  ),
  # PCN
  tar_target(
    readmission_within_28_days_pcn,
    get_readmission_within_28_days_geography(readmission_within_28_days_gp, 
                                             "pcn")
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      readmission_indicator_pcn,
      get_indicators_per_pop(
        readmission_within_28_days_pcn,
        population_pcn,
        "pcn",
        latest_population_year,
        activity_type
      )
    )
  ),
  
  ## Ambulatory Care Conditions ------------------------------------------------
  # LSOA and GP
  tarchetypes::tar_map(
    list(condition = c("acute", "chronic")),
    tar_target(
      ambulatory_care_conditions_lsoa,
      get_ambulatory_care_conditions_sub_geography("lsoa", 
                                                   age_cutoff, 
                                                   start_date, 
                                                   condition,
                                                   con) |>
        join_to_geography_lookup("icb", lsoa_to_higher_geographies)
    )
  ),
  tarchetypes::tar_map(
    list(condition = c("acute", "chronic")),
    tar_target(
      ambulatory_care_conditions_gp,
      get_ambulatory_care_conditions_sub_geography("gp", 
                                                   age_cutoff, 
                                                   start_date, 
                                                   condition,
                                                   con) |>
        join_to_geography_lookup("pcn", gp_to_pcn)
    )
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      ambulatory_care_conditions_acute,
      get_ambulatory_care_conditions_geography(
        ambulatory_care_conditions_lsoa_acute, 
        geography, 
        "acute")
    )
  ),
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      ambulatory_care_conditions_chronic,
      get_ambulatory_care_conditions_geography(
        ambulatory_care_conditions_lsoa_chronic, 
        geography, 
        "chronic")
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      ambulatory_acute_indicator_icb,
      get_indicators_per_pop(
        ambulatory_care_conditions_acute_icb,
        population_icb,
        "icb",
        latest_population_year,
        activity_type
      )
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      ambulatory_chronic_indicator_icb,
      get_indicators_per_pop(
        ambulatory_care_conditions_chronic_icb,
        population_icb,
        "icb",
        latest_population_year,
        activity_type
      )
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      ambulatory_acute_indicator_la,
      get_indicators_per_pop(
        ambulatory_care_conditions_acute_la,
        population_la,
        "la",
        latest_population_year,
        activity_type
      )
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      ambulatory_chronic_indicator_la,
      get_indicators_per_pop(
        ambulatory_care_conditions_chronic_la,
        population_la,
        "la",
        latest_population_year,
        activity_type
      )
    )
  ),
  # PCN
  tar_target(
    ambulatory_care_conditions_acute_pcn,
    get_ambulatory_care_conditions_geography(
      ambulatory_care_conditions_gp_acute,
      "pcn",
      "acute")
  ),
  tar_target(
    ambulatory_care_conditions_chronic_pcn,
    get_ambulatory_care_conditions_geography(
      ambulatory_care_conditions_gp_chronic,
      "pcn",
      "chronic")
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      ambulatory_acute_indicator_pcn,
      get_indicators_per_pop(
        ambulatory_care_conditions_acute_pcn,
        population_pcn,
        "pcn",
        latest_population_year,
        activity_type
      )
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      ambulatory_chronic_indicator_pcn,
      get_indicators_per_pop(
        ambulatory_care_conditions_chronic_pcn,
        population_pcn,
        "pcn",
        latest_population_year,
        activity_type
      )
    )
  ),
  
  ## A&E frequent attenders (adult, ambulance conveyed) ------------------------
  # LSOA and GP
  tar_target(
    frequent_attenders_adult_ambulance_lsoa,
    get_frequent_attenders_adult_ambulance_sub_geography("lsoa", 
                                                         age_cutoff, 
                                                         start_date, 
                                                         con) |>
      # first map 11 to 21 codes and amend numbers according to splits in lsoa:
      dplyr::left_join(lsoa11_to_lsoa_21, 
                       by = c("der_postcode_lsoa_2011_code" = "lsoa11cd")) |>
      dplyr::mutate(
        number = dplyr::n(),
        .by = c(der_postcode_lsoa_2011_code, date),
        frequent_attenders_amended = frequent_attenders / number
      ) |>
      dplyr::rename(der_postcode_lsoa_2021_code = lsoa21cd) |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies)
    ),
  tar_target(
    frequent_attenders_adult_ambulance_gp,
    get_frequent_attenders_adult_ambulance_sub_geography("gp", 
                                                         age_cutoff, 
                                                         start_date, 
                                                         con) |>
      dplyr::rename(gp_practice_sus = gp_practice_code) |>
      join_to_geography_lookup("pcn", gp_to_pcn)
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      frequent_attenders_adult_ambulance,
      get_frequent_attenders_adult_ambulance_geography(
        frequent_attenders_adult_ambulance_lsoa, 
        geography) 
    )
  ),
  tar_target(
    frequent_attenders_indicator_icb,
    get_indicators_per_pop(
      frequent_attenders_adult_ambulance_icb,
      population_icb,
      "icb",
      latest_population_year,
      "attenders"
    )
  ),
  tar_target(
    frequent_attenders_indicator_la,
    get_indicators_per_pop(
      frequent_attenders_adult_ambulance_la,
      population_la,
      "la",
      latest_population_year,
      "attenders"
    )
  ),
  # PCN
  tar_target(
    frequent_attenders_adult_ambulance_pcn,
    get_frequent_attenders_adult_ambulance_geography(
      frequent_attenders_adult_ambulance_gp, 
      "pcn") 
  ),
  tar_target(
    frequent_attenders_indicator_pcn,
    get_indicators_per_pop(
      frequent_attenders_adult_ambulance_pcn,
      population_pcn,
      "pcn",
      latest_population_year,
      "attenders"
    )
  ),
  
  # All indicators -------------------------------------------------------------
  tar_target(
    indicators_icb,
    dplyr::bind_rows(
      elective_non_elective_ratio_icb_admissions, 
      elective_non_elective_ratio_icb_beddays,  
      frailty_indicators_icb_admissions,
      frailty_indicators_icb_beddays,
      readmission_indicator_icb_admissions,
      readmission_indicator_icb_beddays,
      ambulatory_acute_indicator_icb_admissions,
      ambulatory_acute_indicator_icb_beddays,
      ambulatory_chronic_indicator_icb_admissions,
      ambulatory_chronic_indicator_icb_beddays,
      frequent_attenders_indicator_icb
      ) |>
    dplyr::left_join(icb_lookup |>
                       dplyr::select(-dplyr::any_of("geometry")), 
                     "icb") |>
    dplyr::select(indicator, 
                  icb, 
                  icb_name, 
                  date, 
                  numerator, 
                  denominator, 
                  value,
                  lowercl,
                  uppercl)
  ),
  tar_target(
    indicators_la,
    dplyr::bind_rows(
      elective_non_elective_ratio_la_admissions, 
      elective_non_elective_ratio_la_beddays,   
      frailty_indicators_la_admissions,
      frailty_indicators_la_beddays,
      readmission_indicator_la_admissions,
      readmission_indicator_la_beddays,
      ambulatory_acute_indicator_la_admissions,
      ambulatory_acute_indicator_la_beddays,
      ambulatory_chronic_indicator_la_admissions,
      ambulatory_chronic_indicator_la_beddays,
      frequent_attenders_indicator_la
      ) |>
    dplyr::left_join(la_lookup |>
                       dplyr::select(-dplyr::any_of("geometry")), 
                     "la") |>
    dplyr::select(indicator, 
                  la, 
                  la_name, 
                  date, 
                  numerator, 
                  denominator, 
                  value,
                  lowercl,
                  uppercl)
  ),
  tar_target(
    indicators_pcn,
    dplyr::bind_rows(
      elective_non_elective_ratio_pcn_admissions, 
      elective_non_elective_ratio_pcn_beddays,  
      frailty_indicators_pcn_admissions,
      frailty_indicators_pcn_beddays,
      readmission_indicator_pcn_admissions,
      readmission_indicator_pcn_beddays,
      ambulatory_acute_indicator_pcn_admissions,
      ambulatory_acute_indicator_pcn_beddays,
      ambulatory_chronic_indicator_pcn_admissions,
      ambulatory_chronic_indicator_pcn_beddays,
      frequent_attenders_indicator_pcn
      ) |>
    dplyr::left_join(pcn_lookup, "pcn") |>
    dplyr::select(indicator, 
                  pcn, 
                  pcn_name, 
                  date, 
                  numerator, 
                  denominator, 
                  value,
                  lowercl,
                  uppercl)
  )
)
