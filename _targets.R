# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
library(targets)
library(tarchetypes)

# Set target options:
tar_option_set(
  packages = c(
    "tidyverse",
    "stringr",
    "PHEindicatormethods",
    "arrow",
    "glue",
    "pins",
    "rsconnect",
    "nanoparquet",
    "tibble"
  ) # List other packages as needed.
  ##, error = "null" # produce null result even if target errors out
  ##(i.e. continue pipeline)
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

# Pins connection --------------------------------------------------------------
board <- pins::board_connect(server = "connect.strategyunitwm.nhs.uk")

# Dates ------------------------------------------------------------------------
# This section means that monthly indicators will be updated every month to get 
# the latest data. Note: that this does not apply to financial yearly data. 
# FY indicators will need to be manually refreshed when data becomes available.
today <- Sys.Date() 

current_month <- today |>
  lubridate::floor_date("month")

current_financial_year_start <- ifelse(
  lubridate::month(today) < (4 + 3),  # Have added a 3 month lag to allow for FY 
                                      # data to come through
  glue::glue("{lubridate::year(today) - 1}-04-01"),
  glue::glue("{lubridate::year(today)}-04-01")
  ) 

# Replace the target list below with your own:
list(
  # Variables ------------------------------------------------------------------
  ## Age -----------------------------------------------------------------------
  tar_target(age_cutoff, 65),
  tar_target(
    age_bands,
    "('65-69', '70-74', '75-79', '80+', '80-84', '85-89', '85+', '90-94', '95+')"
  ),
  tar_target(
    age_bands_75_plus,
    "('75-79', '80-84', '85-89', '85+', '90-94', '95+')"
  ),
  
  ## Date ----------------------------------------------------------------------
  tar_target(start_date, "2008-04-01"),
  tar_target(
    admissions_lag_date,
    (current_month - months(2)) |>
      as.character()
  ),
  tar_target(
    next_month,
    current_month |>
      lubridate::ceiling_date("month")),
  
  # Lookups --------------------------------------------------------------------
  ## Sub-geographies (LSOA, GP) to higher geographies (ICB, LA, NH) ------------
  tar_target(
    lsoa_to_higher_geographies,
    sf::st_read(
      "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA21_SICBL24_ICB24_CAL24_LAD24_EN_LU/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
    ) |>
      janitor::clean_names() |>
      sf::st_drop_geometry()
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
    pcn_to_nh,
    readxl::read_excel("data/NNHIP Geographies_PCNs_LA_V5.1.xlsx",
                                           sheet = "PCNDetails") |>
      janitor::clean_names() |>
      dplyr::filter(!is.na(nnhip_site)) |>
      dplyr::select(pcn = pcn_code, nnhip_site) |>  # Do we care about last column?
      dplyr::mutate(
        nnhip_code = nnhip_site |>
          factor() |>
          as.numeric(),
        nnhip_code = sprintf(fmt = "NH%03d", nnhip_code)
      ) 
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
  
  tar_target(
    prov_site_type,
    get_eric_site_classifications(con) |>
      dplyr::mutate(
        der_financial_year = stringr::str_replace(
          der_fin_year, 
          "^(\\d{4})\\-(\\d{2})(\\d{2})$", 
          "\\1/\\3")
      )
  ),
  
  ## IMD -----------------------------------------------------------------------
  tar_target(
    imd_2007,
    readxl::read_excel("data/IMD 2007 for DCLG 4 dec.xls", sheet = 2) |>
      janitor::clean_names() |>
      dplyr::mutate(effective_snapshot_date = "2007-12-31") |>
      dplyr::rename(rank = rank_of_imd_where_1_is_most_deprived, 
                    lsoa_code = lsoa) |>
      get_quintile_from_rank()
  ),
  tar_target(
    imd_2010,
    scrape_xls(
      "https://assets.publishing.service.gov.uk/media/5a79b8c6ed915d042206a8e2/1871524.xls",
      sheet = 2
    ) |>
      dplyr::mutate(effective_snapshot_date = "2010-12-31") |>
      dplyr::rename(rank = rank_of_imd_score_where_1_is_most_deprived) |>
      get_quintile_from_rank()
  ),
  tar_target(
    imd_lsoa_lookup,
    DBI::dbGetQuery(
      con,
      "
      SELECT
        LSOA_Code,
        IMD_Decile,
        Effective_Snapshot_Date

      FROM [UKHF_Demography].[Index_Of_Multiple_Deprivation_By_LSOA1_1]
      "
    ) |>
      janitor::clean_names() |>
      unique() |>
      na.omit() |>
      get_quintile_from_decile() |>
      rbind(imd_2010, imd_2007)
  ),
  tar_target(
    earliest_imd_year,
    imd_lsoa_lookup |>
      dplyr::summarise(min(effective_snapshot_date)) |>
      dplyr::pull()
  ),
  tar_target(
    latest_available_imd,
    imd_lsoa_lookup |>
      dplyr::filter(effective_snapshot_date == max(effective_snapshot_date), 
                    .by = lsoa_code) |>
      dplyr::select(lsoa_code, latest_imd_quintile = imd_quintile)
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
    nh_lookup,
    pcn_to_nh |>
      dplyr::select(nh = nnhip_code, nh_name = nnhip_site) |>
      unique()
  ),
  
  # Population data ------------------------------------------------------------
  ## Population restricted to the `age_cutoff` or `age_bands` from above -------
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
      dplyr::left_join(lsoa_to_higher_geographies, by = "lsoa21cd") |>
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
  
  # GP to PCN to NH
  targets::tar_target(
    population_gp_pre_2017_04_01,
    get_population_gp_pre_2017_04_01(age_bands, start_date, con)
  ),
  targets::tar_target(
    population_gp_post_2017_04_01,
    get_population_gp_post_2017_04_01(age_bands, start_date, con)
  ),
  targets::tar_target(
    population_nh,
    get_population_pcn(
      population_gp_pre_2017_04_01,
      population_gp_post_2017_04_01,
      gp_to_pcn
    ) |>
      get_nh_from_pcn(pcn_to_nh) |>
      dplyr::summarise(population_size = sum(population_size),
                       .by = c(date, nnhip_code))
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
      dplyr::left_join(lsoa_to_higher_geographies, by = "lsoa21cd") |>
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
  # GP to PCN to NH (75+)
  targets::tar_target(
    population_75_plus_gp_pre_2017_04_01,
    get_population_gp_pre_2017_04_01(age_bands_75_plus, start_date, con)
  ),
  targets::tar_target(
    population_75_plus_gp_post_2017_04_01,
    get_population_gp_post_2017_04_01(age_bands_75_plus, start_date, con)
  ),
  targets::tar_target(
    population_75_plus_nh,
    get_population_pcn(
      population_75_plus_gp_pre_2017_04_01,
      population_75_plus_gp_post_2017_04_01,
      gp_to_pcn
    ) |>
      get_nh_from_pcn(pcn_to_nh) |>
      dplyr::summarise(population_size = sum(population_size),
                       .by = c(date, nnhip_code))
  ),
  
  ## Population by age range and sex -------------------------------------------
  tar_target(
    population_lsoa_by_age_sex,
    get_population_lsoa_by_age_sex(age_bands, start_date, con)
  ),
  tar_target(
    population_lsoa_mapped_to_higher_geographies_by_age_sex_imd,
    population_lsoa_by_age_sex |>
      dplyr::rename(lsoa11cd = area_code) |>
      dplyr::left_join(lsoa11_to_lsoa_21, by = "lsoa11cd") |>
      dplyr::left_join(lsoa_to_higher_geographies, by = "lsoa21cd") |>
      dplyr::mutate(
        number = dplyr::n(),
        .by = c(lsoa11cd, effective_snapshot_date),
        population_size_amended = population_size / number
      ) |>
      # Adding IMD decile:
      dplyr::mutate(imd_year = effective_snapshot_date, 
                    der_postcode_lsoa_2011_code = lsoa11cd) |>
      get_imd_from_lsoa(imd_lsoa_lookup, 
                        earliest_imd_year, 
                        latest_available_imd) |>
      dplyr::select(-imd_year, -der_postcode_lsoa_2011_code)
  ),
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      population_by_age_sex_imd,
      get_population_higher_geography_from_lsoa_by_age_sex_imd(
        population_lsoa_mapped_to_higher_geographies_by_age_sex_imd,
        geography
      )
    )
  ),
  # GP to PCN to NH
  targets::tar_target(
    population_gp_pre_2017_04_01_by_age_sex,
    get_population_gp_pre_2017_04_01_by_age_sex(age_bands, start_date, con)
  ),
  targets::tar_target(
    population_gp_post_2017_04_01_by_age_sex,
    get_population_gp_post_2017_04_01_by_age_sex(age_bands, start_date, con)
  ),
  targets::tar_target(
    population_by_age_sex_nh,
    get_population_pcn_by_age_sex(
      population_gp_pre_2017_04_01_by_age_sex,
      population_gp_post_2017_04_01_by_age_sex,
      gp_to_pcn
    ) |>
      get_nh_from_pcn(pcn_to_nh) |>
      dplyr::summarise(population_size = sum(population_size),
                       .by = c(date, nnhip_code, age_range, sex))
  ),
  targets::tar_target(
    population_by_age_sex_imd_nh,
    population_by_age_sex_nh |>
      dplyr::mutate(
        date_full = lubridate::ymd(date, truncated = 1),
        nh_prop_date = ifelse(
          lubridate::month(date_full) < 7,
          glue::glue("{lubridate::year(date_full) - 1}-07"),
          glue::glue("{lubridate::year(date_full)}-07")
        )
      ) |>
      dplyr::left_join(nh_population_proportional_imd, 
                       by = c("nh_prop_date", "nnhip_code" = "nh")) |>
      dplyr::mutate(population_size = population_size * prop) |>
      dplyr::select(date, nh = nnhip_code, age_range, sex, imd_quintile, population_size)
  ),
  ## GP, PCN and NH proportionally by IMD --------------------------------------
  tar_target(
    gp_to_lsoa_population,
    DBI::dbGetQuery(
      con,
      "
      SELECT
        [GP_Practice_Code],
        [LSOA_Code],
        [Effective_Snapshot_Date],
        SUM(Size) AS population_size

      FROM [UKHF_Demography].[No_Of_Patients_Regd_At_GP_Practice_LSOA_Level1_1]

      WHERE Sex != 'ALL'

      GROUP BY
        [GP_Practice_Code],
        [LSOA_Code],
        [Effective_Snapshot_Date]
      "
    ) |>
      janitor::clean_names() |>
      unique()
  ),
  tar_target(
    nh_population_proportional_imd,
    gp_to_lsoa_population |>
      dplyr::filter(lubridate::month(effective_snapshot_date) == 7) |>
      dplyr::mutate(imd_year = effective_snapshot_date, 
                    der_postcode_lsoa_2011_code = lsoa_code) |>
      get_imd_from_lsoa(imd_lsoa_lookup, earliest_imd_year, latest_available_imd) |>
      dplyr::left_join(
        gp_to_pcn,
        by = c("gp_practice_code" = "partner_organisation_code")
      ) |>
      get_nh_from_pcn(pcn_to_nh) |>
      dplyr::summarise(
        population_size = sum(population_size),
        .by = c(nnhip_code, effective_snapshot_date, imd_quintile)
      ) |>
      dplyr::filter(!is.na(imd_quintile)) |>
      dplyr::mutate(
        prop = population_size / sum(population_size),
        .by = c(nnhip_code, effective_snapshot_date)
      ) |>
      dplyr::mutate(
        nh_prop_date = stringr::str_sub(effective_snapshot_date, 1, 7)
        ) |>
      dplyr::select(nh = nnhip_code, nh_prop_date, imd_quintile, prop)
  ),
  
  ## England census ------------------------------------------------------------
  tar_target(
    census_url,
    "https://www.ons.gov.uk/visualisations/dvc1914/fig4/datadownload.xlsx"
  ),
  tar_target(
    standard_england_pop_2021_census,
    scrape_xls(census_url, skip = 5) |>
      janitor::clean_names() |>
      dplyr::select(age, dplyr::contains("2021")) |>
      tidyr::pivot_longer(
        dplyr::contains("2021"),
        names_to = "sex",
        values_to = "pop"
      ) |>
      dplyr::mutate(
        sex = ifelse(stringr::str_detect(sex, "female"), "2", "1"),
        age_range = age |>
          stringr::str_replace_all(
            c(
              "Aged " = "",
              " to " = "-",
              " years" = "",
              "4 and under" = "0-4",
              "90 and over" = "80+",
              "80-84" = "80+",
              "85-89" = "80+"
            )
          )
      ) |>
      na.omit() |>
      dplyr::summarise(pop = sum(pop), .by = c(age_range, sex))
  ),
  tar_target(
    standard_pop_by_age_sex_imd,
    read.csv("data/esp13_imd_alt.csv") |>
      janitor::clean_names() |>
      dplyr::mutate(
        sex = ifelse(stringr::str_detect(gender, "Female"), "2", "1"),
        age_range = stringr::str_remove(age, " years"),
        age_range = ifelse(age_range %in% c("80-84", "85-89", "90plus"), 
                           "80+", 
                           age_range)
      ) |>
      dplyr::summarise(
        pop = sum(esp13_pop_part_2, na.rm = TRUE),
        .by = c(age_range, sex, imd_quintile)
      )
  ),
  
  ## Distribution of SUS provider activity by ICB, LAD and NH for bed data -----
  tar_target(prov_act_dist_geog, get_prov_dist_by_lsoa(con)),
  tar_target(
    prov_act_dist_icb,
    prov_act_dist_geog |>
      dplyr::filter(prov_code != "", lsoa_2011 != "") |>
      dplyr::left_join(
        lsoa11_to_lsoa_21 |>
          dplyr::select(1, 2),
        by = c("lsoa_2011" = "lsoa11cd"),
        relationship = "many-to-many"
      ) |>
      dplyr::left_join(
        lsoa_to_higher_geographies |>
          dplyr::select(1, 6:8),
        by = "lsoa21cd",
        relationship = "many-to-many"
      ) |>
      dplyr::filter(!is.na(icb24cd)) |>
      dplyr::group_by(prov_code,
                      der_financial_year, 
                      icb24cd, 
                      icb24cdh, 
                      icb24nm) |>
      dplyr::summarise(patients = sum(patients), beddays = sum(beddays)) |>
      dplyr::group_by(prov_code, der_financial_year) |>
      dplyr::mutate(
        prop_pat = patients / sum(patients),
        prop_bed = beddays / sum(beddays)
      )
  ),
  tar_target(
    prov_act_dist_lad,
    prov_act_dist_geog |>
      dplyr::filter(prov_code != "", lsoa_2011 != "") |>
      dplyr::left_join(
        lsoa11_to_lsoa_21 |>
          dplyr::select(1, 2),
        by = c("lsoa_2011" = "lsoa11cd"),
        relationship = "many-to-many"
      ) |>
      dplyr::left_join(
        lsoa_to_higher_geographies |>
          dplyr::select(1, 11:12),
        by = "lsoa21cd",
        relationship = "many-to-many"
      ) |>
      dplyr::filter(!is.na(lad24cd)) |>
      dplyr::group_by(prov_code, der_financial_year, lad24cd, lad24nm) |>
      dplyr::summarise(patients = sum(patients), beddays = sum(beddays)) |>
      dplyr::group_by(prov_code, der_financial_year) |>
      dplyr::mutate(
        prop_pat = patients / sum(patients),
        prop_bed = beddays / sum(beddays)
      )
  ),
  
  tar_target(prov_act_dist_prac, get_prov_dist_by_practice(con)),
  tar_target(
    prov_act_dist_nh,
    prov_act_dist_prac |>
      dplyr::filter(prov_code != "", gp_prac != "") |>
      dplyr::left_join(
        gp_to_pcn |>
          dplyr::select(1:2, 5:6),
        by = c("gp_prac" = "partner_organisation_code"),
        relationship = "many-to-many"
      ) |>
      dplyr::filter(!is.na(pcn_code)) |>
      get_nh_from_pcn(pcn_to_nh) |>
      dplyr::group_by(prov_code, der_financial_year, nnhip_code) |>
      dplyr::summarise(patients = sum(patients), beddays = sum(beddays)) |>
      dplyr::group_by(prov_code, der_financial_year) |>
      dplyr::mutate(
        prop_pat = patients / sum(patients),
        prop_bed = beddays / sum(beddays)
      )
  ),
  
  ## Distribution of provider patients (SUS,CSDS,MHSDS) for workforce and costs data -----
  tar_target(
    prov_pat_dist_lsoa,
    get_prov_pats_lsoa("data/prov_lsoa_pats.csv") |>
      dplyr::left_join(lsoa11_to_lsoa_21, by = c("lsoa_2011" = "lsoa11cd")) |>
      dplyr::left_join(lsoa_to_higher_geographies |>
                         select(1, 6:8, 11:12), by = "lsoa21cd") |>
      janitor::clean_names()
  ),
  tar_target(
    prov_pat_dist_icb,
    prov_pat_dist_lsoa |>
      dplyr::group_by(prov_code, icb24cdh, icb24nm, der_financial_year) |>
      dplyr::summarise(pats = sum(pats)) |>
      dplyr::group_by(prov_code, der_financial_year) |>
      dplyr::mutate(prop_pat = pats / sum(pats))
  ),
  tar_target(
    prov_pat_dist_lad,
    prov_pat_dist_lsoa |>
      dplyr::group_by(prov_code, lad24cd, lad24nm, der_financial_year) |>
      dplyr::summarise(pats = sum(pats)) |>
      dplyr::group_by(prov_code, der_financial_year) |>
      dplyr::mutate(prop_pat = pats / sum(pats))
  ),
  
  tar_target(
    prov_pat_dist_prac,
    get_prov_pats_gpprac("data/prov_gpprac_pats.csv") |>
      janitor::clean_names()
  ),
  tar_target(
    prov_pat_dist_nh,
    prov_pat_dist_prac |>
      dplyr::filter(prov_code != "", gp_prac != "") |>
      dplyr::left_join(
        gp_to_pcn |>
          dplyr::select(1:2, 5:6),
        by = c("gp_prac" = "partner_organisation_code"),
        relationship = "many-to-many"
      ) |>
      dplyr::filter(!is.na(pcn_code)) |>
      get_nh_from_pcn(pcn_to_nh) |>
      dplyr::group_by(prov_code, der_financial_year, nnhip_code) |>
      dplyr::summarise(patients = sum(pats)) |>
      ungroup() |>
      dplyr::group_by(prov_code, der_financial_year) |>
      dplyr::mutate(prop_pat = patients / sum(patients))
  ),
  
  # Indicators -----------------------------------------------------------------
  ## Elective to non elective admissions ratio ---------------------------------
  # LSOA and GP
  tar_target(
    elective_non_elective_lsoa,
    get_elective_non_elective_sub_geography("lsoa",
                                            age_cutoff,
                                            start_date,
                                            admissions_lag_date,
                                            con) |>
      recode_lsoa11_as_lsoa21(lsoa11_to_lsoa_21, "admissions") |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies)
  ),
  tar_target(
    elective_non_elective_gp,
    get_elective_non_elective_sub_geography("gp",
                                            age_cutoff,
                                            start_date,
                                            admissions_lag_date,
                                            con) |>
      join_to_geography_lookup("pcn", gp_to_pcn) |>
      get_nh_from_pcn(pcn_to_nh)
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(
      geography = rep(c("icb", "la"), 2),
      activity_type = rep(c("admissions", "beddays"), each = 2)
    ),
    tar_target(
      elective_non_elective_ratio,
      get_elective_non_elective_ratio(elective_non_elective_lsoa,
                                      geography,
                                      activity_type) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),
  # NH
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      elective_non_elective_ratio_nh,
      get_elective_non_elective_ratio(elective_non_elective_gp,
                                      "nh",
                                      activity_type) |>
        dplyr::mutate(frequency = "monthly")
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
  tar_target(
    frailty_episodes,
    get_frailty_data(start_date, admissions_lag_date, con)
  ),
  # LSOA and GP
  tar_target(
    frailty_episodes_with_risk_scores,
    get_frailty_with_risk_scores(frailty_episodes, frailty_risk_scores)
  ),
  tarchetypes::tar_map(
    list(sub_geography = c("gp", "lsoa")),
    tar_target(
      frailty,
      get_frailty_sub_geography(frailty_episodes_with_risk_scores,
                                sub_geography)
    )
  ),
  tar_target(
    frailty_lsoa_2021,
    frailty_lsoa |>
      recode_lsoa11_as_lsoa21(lsoa11_to_lsoa_21, "admissions")
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      frailty,
      get_frailty_geography(frailty_lsoa_2021,
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
      ) |>
        dplyr::mutate(frequency = "monthly")
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
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),
  # NH
  tar_target(
    frailty_nh,
    get_frailty_geography(frailty_gp, "nh", gp_to_pcn, pcn_to_nh)
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      frailty_indicators_nh,
      get_indicators_per_pop(
        frailty_nh,
        population_75_plus_nh,
        "nh",
        latest_population_year,
        activity_type
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),

  ## Emergency readmission within 28 days --------------------------------------
  tar_target(
    readmissions_where_clause,
    "a.Der_Pseudo_NHS_number IS NOT NULL AND
    (a.Spell_Core_HRG!= 'PB03Z' OR Spell_Core_HRG IS NULL) AND NOT
    (Treatment_Function_Code = '424') AND
    EXISTS (
    	SELECT 1

    	FROM [Reporting_MESH_APC].[APCE_Core_Monthly_Snapshot]  b

    	WHERE

    	 a.Der_Pseudo_NHS_Number = b.Der_Pseudo_NHS_Number AND
    	 DATEDIFF(DD, b.Discharge_Date, a.Admission_Date) BETWEEN 0 AND 28 AND
    	 (b.Admission_Date < a.Admission_Date OR
    	  b.Discharge_Date < a.Discharge_Date) AND
    	  a.APCE_Ident != b.APCE_Ident
     )
    "
  ),
  # LSOA and GP
  tar_target(
    readmission_within_28_days_episodes,
    get_emergency_indicator_episodes(
      age_cutoff,
      start_date,
      admissions_lag_date,
      readmissions_where_clause,
      con
    )
  ),
  tar_target(
    readmission_within_28_days_lsoa,
    readmission_within_28_days_episodes |>
      get_indicator_at_sub_geography_level("lsoa") |>
      recode_lsoa11_as_lsoa21(lsoa11_to_lsoa_21, "admissions") |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies)
  ),
  tar_target(
    readmission_within_28_days_gp,
    readmission_within_28_days_episodes |>
      get_indicator_at_sub_geography_level("gp") |>
      join_to_geography_lookup("pcn", gp_to_pcn) |>
      get_nh_from_pcn(pcn_to_nh)
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      readmission_within_28_days,
      aggregate_indicator_to_geography_level(
        readmission_within_28_days_lsoa,
        geography,
        "readmission_within_28_days"
      )
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
      ) |>
        dplyr::mutate(frequency = "monthly")
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
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),
  # NH
  tar_target(
    readmission_within_28_days_nh,
    aggregate_indicator_to_geography_level(
      readmission_within_28_days_gp,
      "nh",
      "readmission_within_28_days"
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      readmission_indicator_nh,
      get_indicators_per_pop(
        readmission_within_28_days_nh,
        population_nh,
        "nh",
        latest_population_year,
        activity_type
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),

  ## Ambulatory Care Conditions ------------------------------------------------
  ### Acute --------------------------------------------------------------------
  tar_target(
    ambulatory_care_acute_where_clause,
    "(
    ((Der_Primary_Diagnosis_Code LIKE 'L0[34]%' OR
    Der_Primary_Diagnosis_Code LIKE 'L08[089]%' OR
    Der_Primary_Diagnosis_Code LIKE 'L88X%' OR
    Der_Primary_Diagnosis_Code LIKE 'L980%' ) AND
    (Der_Procedure_All NOT LIKE '%[ABCDEFGHJKLMNOPQRTVW]%' AND
    Der_Procedure_All NOT LIKE '%S[123]%' AND
    Der_Procedure_All NOT LIKE '%S4[1234589]%' AND
    Der_Procedure_All NOT LIKE '%X0[1245]%' ) ) OR ----cellulitis

    (Der_Primary_Diagnosis_Code LIKE 'G4[01]%' OR
     Der_Primary_Diagnosis_Code LIKE 'O15%' OR
     Der_Primary_Diagnosis_Code LIKE 'R56%' ) OR ---Convulsions and epilepsy

    (Der_Primary_Diagnosis_Code LIKE 'E86X%' OR
     Der_Primary_Diagnosis_Code LIKE 'K52[289]%') OR ---dehydration_and_gastroenteritis

    (Der_Primary_Diagnosis_Code LIKE 'A690)%' OR
     Der_Primary_Diagnosis_Code LIKE 'K0[2-68]%' OR
     Der_Primary_Diagnosis_Code LIKE 'K09[89]%' OR
     Der_Primary_Diagnosis_Code LIKE 'K1[23]%') OR ---dental_conditions

    (Der_Primary_Diagnosis_Code LIKE 'H6[67]%' OR
      Der_Primary_Diagnosis_Code LIKE 'J0[236]%' OR
      Der_Primary_Diagnosis_Code LIKE 'J312%' ) OR ---ent_infections

    (Der_Primary_Diagnosis_Code LIKE 'N7[034]%') OR ---pelvic_inflammatory_disease

    (Der_Primary_Diagnosis_Code LIKE 'K2[5678][012456]%') OR ---perforated_bleeding_ulcer

    (Der_Primary_Diagnosis_Code LIKE 'N1[012]%' OR
      Der_Primary_Diagnosis_Code LIKE 'N136%' ) OR ---pyelonephritis

    Der_Diagnosis_All LIKE '%R02X%' --gangrene
    )
    "
  ),
  # LSOA and GP
  tar_target(
    ambulatory_care_conditions_acute_episodes,
    get_emergency_indicator_episodes(
      age_cutoff,
      start_date,
      admissions_lag_date,
      ambulatory_care_acute_where_clause,
      con
    )
  ),
  tar_target(
    ambulatory_care_conditions_acute_lsoa,
    ambulatory_care_conditions_acute_episodes |>
      get_indicator_at_sub_geography_level("lsoa") |>
      recode_lsoa11_as_lsoa21(lsoa11_to_lsoa_21, "admissions") |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies)
  ),
  tar_target(
    ambulatory_care_conditions_acute_gp,
    ambulatory_care_conditions_acute_episodes |>
      get_indicator_at_sub_geography_level("gp") |>
      join_to_geography_lookup("pcn", gp_to_pcn) |>
      get_nh_from_pcn(pcn_to_nh)
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      ambulatory_care_conditions_acute,
      aggregate_indicator_to_geography_level(
        ambulatory_care_conditions_acute_lsoa,
        geography,
        "ambulatory_care_conditions_acute"
      )
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
      ) |>
        dplyr::mutate(frequency = "monthly")
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
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),
  # NHP
  tar_target(
    ambulatory_care_conditions_acute_nh,
    aggregate_indicator_to_geography_level(
      ambulatory_care_conditions_acute_gp,
      "nh",
      "ambulatory_care_conditions_acute"
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      ambulatory_acute_indicator_nh,
      get_indicators_per_pop(
        ambulatory_care_conditions_acute_nh,
        population_nh,
        "nh",
        latest_population_year,
        activity_type
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),

  ### Chronic ------------------------------------------------------------------
  tar_target(
    ambulatory_care_chronic_where_clause,
    "(
    ((Der_Primary_Diagnosis_Code LIKE 'I20%' OR
    Der_Primary_Diagnosis_Code LIKE 'I24[089]%' ) AND
    ((Der_Procedure_All NOT LIKE '[ABCDEFGHJKLMNOPQRSTVW]%' AND
    Der_Procedure_All NOT LIKE 'X0[1245]%' ) OR
    Der_Procedure_All IS NULL )) OR ----angina

    (Der_Primary_Diagnosis_Code LIKE 'J4[56]%') OR ---asthma

    ((Der_Primary_Diagnosis_Code LIKE 'I110%' OR
    Der_Primary_Diagnosis_Code LIKE 'I50%' OR
    Der_Primary_Diagnosis_Code LIKE 'I10%' OR
    Der_Primary_Diagnosis_Code LIKE 'I119%' OR
    Der_Primary_Diagnosis_Code LIKE 'J81%' ) AND
    ((Der_Procedure_All NOT LIKE '%K[0-4]%' AND
    Der_Procedure_All NOT LIKE '%K5[02567]%' AND
    Der_Procedure_All NOT LIKE '%K6[016789]%'  AND
    Der_Procedure_All NOT LIKE '%K71%' ) OR
    Der_Procedure_All IS NULL) ) OR -----congestive_heart_failure / hypertension

    (Der_Primary_Diagnosis_Code LIKE 'J4[12347]%' OR
    (Der_Primary_Diagnosis_Code LIKE 'J20%' AND
    Der_Diagnosis_All LIKE '%J4[12347]%' )) OR ----copd

    (Der_Primary_Diagnosis_Code LIKE 'D50[189]%') OR ---iron-deficiency_anaemia

    (Der_Primary_Diagnosis_Code LIKE 'E4[0123]X%' OR
    Der_Primary_Diagnosis_Code LIKE 'E550%' OR
    Der_Primary_Diagnosis_Code LIKE 'E643%') OR ---nutritional_deficiencies

    (Der_Diagnosis_All LIKE '%E10[0-8]%' OR
    Der_Diagnosis_All LIKE '%E11[0-8]%' OR
    Der_Diagnosis_All LIKE '%E12[0-8]%' OR
    Der_Diagnosis_All LIKE '%E13[0-8]%' OR
    Der_Diagnosis_All LIKE '%E14[0-8]%' ) --- diabetes complications
    )
    "
  ),
  # LSOA and GP
  tar_target(
    ambulatory_care_conditions_chronic_episodes,
    get_emergency_indicator_episodes(
      age_cutoff,
      start_date,
      admissions_lag_date,
      ambulatory_care_chronic_where_clause,
      con
    )
  ),
  tar_target(
    ambulatory_care_conditions_chronic_lsoa,
    ambulatory_care_conditions_chronic_episodes |>
      get_indicator_at_sub_geography_level("lsoa") |>
      recode_lsoa11_as_lsoa21(lsoa11_to_lsoa_21, "admissions") |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies)
  ),
  tar_target(
    ambulatory_care_conditions_chronic_gp,
    ambulatory_care_conditions_chronic_episodes |>
      get_indicator_at_sub_geography_level("gp") |>
      join_to_geography_lookup("pcn", gp_to_pcn) |>
      get_nh_from_pcn(pcn_to_nh)
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      ambulatory_care_conditions_chronic,
      aggregate_indicator_to_geography_level(
        ambulatory_care_conditions_chronic_lsoa,
        geography,
        "ambulatory_care_conditions_chronic"
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
      ) |>
        dplyr::mutate(frequency = "monthly")
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
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),
  # NHP
  tar_target(
    ambulatory_care_conditions_chronic_nh,
    aggregate_indicator_to_geography_level(
      ambulatory_care_conditions_chronic_gp,
      "nh",
      "ambulatory_care_conditions_chronic"
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      ambulatory_chronic_indicator_nh,
      get_indicators_per_pop(
        ambulatory_care_conditions_chronic_nh,
        population_nh,
        "nh",
        latest_population_year,
        activity_type
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),

  ### Vaccine preventable ------------------------------------------------------
  tar_target(
    ambulatory_care_vaccine_preventable_where_clause,
    "(
    ((Der_Diagnosis_All LIKE '%J1[0134]%' OR
    Der_Diagnosis_All LIKE '%J15[3479]%' OR
    Der_Diagnosis_All LIKE '%J168%' OR
    Der_Diagnosis_All LIKE '%J18[18]%'  ) AND
    (Der_Diagnosis_All NOT LIKE '%D57%')) OR ----influenza_and_pneumonia

    (Der_Diagnosis_All LIKE '%A3[567]%' OR
    Der_Diagnosis_All LIKE '%A80%' OR
    Der_Diagnosis_All LIKE '%B0[56]%' OR
    Der_Diagnosis_All LIKE '%B16[19]%' OR
    Der_Diagnosis_All LIKE '%B18[01]%' OR
    Der_Diagnosis_All LIKE '%B26%' OR
    Der_Diagnosis_All LIKE '%G000%' OR
    Der_Diagnosis_All LIKE '%M014%') ---other
    )
    "
  ),
  # LSOA and GP
  tar_target(
    ambulatory_care_conditions_vaccine_preventable_episodes,
    get_emergency_indicator_episodes(
      age_cutoff,
      start_date,
      admissions_lag_date,
      ambulatory_care_vaccine_preventable_where_clause,
      con
    )
  ),
  tar_target(
    ambulatory_care_conditions_vaccine_preventable_lsoa,
    ambulatory_care_conditions_vaccine_preventable_episodes |>
      get_indicator_at_sub_geography_level("lsoa") |>
      recode_lsoa11_as_lsoa21(lsoa11_to_lsoa_21, "admissions") |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies)
  ),
  tar_target(
    ambulatory_care_conditions_vaccine_preventable_gp,
    ambulatory_care_conditions_vaccine_preventable_episodes |>
      get_indicator_at_sub_geography_level("gp") |>
      join_to_geography_lookup("pcn", gp_to_pcn) |>
      get_nh_from_pcn(pcn_to_nh)
  ),

  ## A&E frequent attenders (adult, ambulance conveyed) ------------------------
  # LSOA and GP
  tar_target(
    frequent_attenders_adult_ambulance_lsoa_current,
    get_frequent_attenders_adult_ambulance_sub_geography_current(
      "lsoa",
      age_cutoff,
      admissions_lag_date,
      con
      )
  ),
  tar_target(
    frequent_attenders_adult_ambulance_gp_current,
    get_frequent_attenders_adult_ambulance_sub_geography_current(
      "gp",
      age_cutoff,
      admissions_lag_date,
      con
      )
  ),
  tar_target(
    frequent_attenders_adult_ambulance_lsoa_archived,
    get_frequent_attenders_adult_ambulance_sub_geography_archived("lsoa",
                                                                  age_cutoff,
                                                                  start_date,
                                                                  con)
  ),
  tar_target(
    frequent_attenders_adult_ambulance_gp_archived,
    get_frequent_attenders_adult_ambulance_sub_geography_archived("gp",
                                                                  age_cutoff,
                                                                  start_date,
                                                                  con)
  ),
  tar_target(
    frequent_attenders_adult_ambulance_lsoa,
    rbind(
      frequent_attenders_adult_ambulance_lsoa_current,
      frequent_attenders_adult_ambulance_lsoa_archived
    ) |>
      recode_lsoa11_as_lsoa21(lsoa11_to_lsoa_21, "frequent_attenders") |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies)
  ),
  tar_target(
    frequent_attenders_adult_ambulance_gp,
    rbind(
      frequent_attenders_adult_ambulance_gp_current,
      frequent_attenders_adult_ambulance_gp_archived
    ) |>
      dplyr::rename(gp_practice_sus = gp_practice_code) |>
      join_to_geography_lookup("pcn", gp_to_pcn) |>
      get_nh_from_pcn(pcn_to_nh)
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
    ) |>
      dplyr::mutate(frequency = "monthly")
  ),
  tar_target(
    frequent_attenders_indicator_la,
    get_indicators_per_pop(
      frequent_attenders_adult_ambulance_la,
      population_la,
      "la",
      latest_population_year,
      "attenders"
    ) |>
      dplyr::mutate(frequency = "monthly")
  ),
  # NHP
  tar_target(
    frequent_attenders_adult_ambulance_nh,
    get_frequent_attenders_adult_ambulance_geography(
      frequent_attenders_adult_ambulance_gp,
      "nh")
  ),
  tar_target(
    frequent_attenders_indicator_nh,
    get_indicators_per_pop(
      frequent_attenders_adult_ambulance_nh,
      population_nh,
      "nh",
      latest_population_year,
      "attenders"
    ) |>
      dplyr::mutate(frequency = "monthly")
  ),

  ## Mental Health Admissions via ED -------------------------------------------
  # LSOA and GP
  tar_target(
    raid_ae_lsoa,
    get_raid_ae_sub_geography("lsoa",
                              age_cutoff,
                              start_date,
                              admissions_lag_date,
                              con) |>
      recode_lsoa11_as_lsoa21(lsoa11_to_lsoa_21, "admissions") |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies)
  ),
  tar_target(
    raid_ae_gp,
    get_raid_ae_sub_geography("gp",
                              age_cutoff,
                              start_date,
                              admissions_lag_date,
                              con) |>
      join_to_geography_lookup("pcn", gp_to_pcn) |>
      get_nh_from_pcn(pcn_to_nh)
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      raid_ae,
      aggregate_indicator_to_geography_level(raid_ae_lsoa, geography, "raid_ae")
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      raid_ae_indicator_icb,
      get_indicators_per_pop(
        raid_ae_icb,
        population_icb,
        "icb",
        latest_population_year,
        activity_type
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      raid_ae_indicator_la,
      get_indicators_per_pop(
        raid_ae_la,
        population_la,
        "la",
        latest_population_year,
        activity_type
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),
  # NH
  tar_target(
    raid_ae_nh,
    aggregate_indicator_to_geography_level(raid_ae_gp, "nh", "raid_ae")
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      raid_ae_indicator_nh,
      get_indicators_per_pop(
        raid_ae_nh,
        population_nh,
        "nh",
        latest_population_year,
        activity_type
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),

  ## Emergency hospital admissions due to falls in people over 65 --------------
  tar_target(
    falls_where_clause,
    "(LEFT(Der_Primary_Diagnosis_Code, 4) = 'R296' OR ----tendency to fall
      ((Der_Primary_Diagnosis_Code LIKE 'S%' OR
        Der_Primary_Diagnosis_Code LIKE 'T%') AND
        Der_Diagnosis_All LIKE '%W[01]%') OR   ----explicit_fractures
      ((Der_Diagnosis_All LIKE '%M48[45]%' OR
      Der_Diagnosis_All LIKE '%M80[01234589]%' OR
      Der_Diagnosis_All LIKE '%S22[01]%' OR
      Der_Diagnosis_All LIKE '%S32[012347]%' OR
      Der_Diagnosis_All LIKE '%S42[234]%' OR
      Der_Diagnosis_All LIKE '%S52%' OR
      Der_Diagnosis_All LIKE '%S620%' OR
      Der_Diagnosis_All LIKE '%S72[012348]%' OR
      Der_Diagnosis_All LIKE '%T08X%' ) AND
      Der_Diagnosis_All NOT LIKE '%[VWXY]%') ----implicit_fractures
    ) AND
    Der_Procedure_Count >= 1 AND
    DATEDIFF(day, Admission_Date, Discharge_Date) >= 2
    "
  ),
  # LSOA and GP
  tar_target(
    falls_related_admissions_episodes,
    get_emergency_indicator_episodes(
      age_cutoff,
      start_date,
      admissions_lag_date,
      falls_where_clause,
      con
    )
  ),
  tar_target(
    falls_related_admissions_lsoa,
    falls_related_admissions_episodes |>
      get_indicator_at_sub_geography_level("lsoa") |>
      recode_lsoa11_as_lsoa21(lsoa11_to_lsoa_21, "admissions") |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies)
  ),
  tar_target(
    falls_related_admissions_gp,
    falls_related_admissions_episodes |>
      get_indicator_at_sub_geography_level("gp") |>
      join_to_geography_lookup("pcn", gp_to_pcn) |>
      get_nh_from_pcn(pcn_to_nh)
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      falls_related_admissions,
      aggregate_indicator_to_geography_level(
        falls_related_admissions_lsoa,
        geography,
        "falls_related_admissions"
      )
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      falls_indicator_icb,
      get_indicators_per_pop(
        falls_related_admissions_icb,
        population_icb,
        "icb",
        latest_population_year,
        activity_type
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      falls_indicator_la,
      get_indicators_per_pop(
        falls_related_admissions_la,
        population_la,
        "la",
        latest_population_year,
        activity_type
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),
  # NH
  tar_target(
    falls_related_admissions_nh,
    aggregate_indicator_to_geography_level(
      falls_related_admissions_gp,
      "nh",
      "falls_related_admissions"
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      falls_indicator_nh,
      get_indicators_per_pop(
        falls_related_admissions_nh,
        population_nh,
        "nh",
        latest_population_year,
        activity_type
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),

  ## Redirection / Substitution ------------------------------------------------
  tar_target(
    zero_los_no_procedure_where_clause,
    "Discharge_Method IN ('1', '2', '3') AND
    DATEDIFF(day, Admission_Date, Discharge_Date) = 0 AND
    (Der_Procedure_Count = 0 OR Der_Procedure_All IS NULL)
    "
  ),
  tar_target(
    medicines_related_admissions_where_clause,
    "(Der_Diagnosis_All LIKE '%Y4%' OR
      Der_Diagnosis_All LIKE '%Y5[01234567]%') OR ---explicit

    ((Der_Primary_Diagnosis_Code LIKE 'E16[012]%' OR
      Der_Primary_Diagnosis_Code LIKE 'E781%' OR
      Der_Primary_Diagnosis_Code LIKE 'R55X%' OR
      Der_Primary_Diagnosis_Code LIKE 'R739%' OR
      Der_Primary_Diagnosis_Code LIKE 'T383%') AND
      (Der_Diagnosis_All LIKE '%E1[01234]%') AND
      (Der_Diagnosis_All NOT LIKE '%Y4%' AND
      Der_Diagnosis_All NOT LIKE '%Y5[01234567]%')) OR ---implicit - anti-diabetics

    ((Der_Primary_Diagnosis_Code LIKE 'R55X%' OR
      Der_Primary_Diagnosis_Code LIKE 'S060%' OR
      Der_Primary_Diagnosis_Code LIKE 'S52[012345678]%' OR
      Der_Primary_Diagnosis_Code LIKE 'S628%' OR
      Der_Primary_Diagnosis_Code LIKE 'S720%' OR
      Der_Primary_Diagnosis_Code LIKE 'W%') AND
      (Der_Diagnosis_All LIKE '%F%') AND
      (Der_Diagnosis_All NOT LIKE '%Y4%' AND
      Der_Diagnosis_All NOT LIKE '%Y5[01234567]%')) OR ---implicit - benzodiasepines

    ((Der_Primary_Diagnosis_Code LIKE 'E86X%' OR
       Der_Primary_Diagnosis_Code LIKE 'E87[56]%' OR
       Der_Primary_Diagnosis_Code LIKE 'I470%' OR
       Der_Primary_Diagnosis_Code LIKE 'I49[89]%' OR
       Der_Primary_Diagnosis_Code LIKE 'R55X%' OR
       Der_Primary_Diagnosis_Code LIKE 'R571%') AND
       (Der_Diagnosis_All LIKE '%I10X%' OR
       Der_Diagnosis_All LIKE '%I1[12][09]%' OR
       Der_Diagnosis_All LIKE '%I13[01239]%' OR
       Der_Diagnosis_All LIKE '%I150%') AND
       (Der_Diagnosis_All NOT LIKE '%Y4%' AND
       Der_Diagnosis_All NOT LIKE '%Y5[01234567]%')) OR ---implicit - diurectics

    ((Der_Primary_Diagnosis_Code LIKE 'E87[56]%' OR
      Der_Primary_Diagnosis_Code LIKE 'I50[019]%' OR
      Der_Primary_Diagnosis_Code LIKE 'K25[059]%' OR
      Der_Primary_Diagnosis_Code LIKE 'K922%' OR
      Der_Primary_Diagnosis_Code LIKE 'R10[34]%') AND
      (Der_Diagnosis_All LIKE '%M05[389]%' OR
      Der_Diagnosis_All LIKE '%M06[089]%' OR
      Der_Diagnosis_All LIKE '%M080%' OR
      Der_Diagnosis_All LIKE '%M15[0123489]%' OR
      Der_Diagnosis_All LIKE '%M16[012345679]%' OR
      Der_Diagnosis_All LIKE '%M1[78][0123459]%' OR
      Der_Diagnosis_All LIKE '%M19[01289]%') AND
      (Der_Diagnosis_All NOT LIKE '%Y4%' AND
      Der_Diagnosis_All NOT LIKE '%Y5[0-7]%')) ---implicit - nsaids
    "
  ),
  tar_target(
    redirection_where_clause,
    paste(
      zero_los_no_procedure_where_clause,
      medicines_related_admissions_where_clause,
      sep = " AND "
    )
  ),
  tar_target(
    zero_los_and_medicine_related_episodes,
    get_emergency_indicator_episodes(
      age_cutoff,
      start_date,
      admissions_lag_date,
      redirection_where_clause,
      con
    )
  ),

  tar_target(
    end_of_life_episodes,
    get_end_of_life_episodes(age_cutoff, start_date, admissions_lag_date, con)
  ),

  # LSOA and GP
  tar_target(
    redirection_episodes,
    # Combining all the redirection mitigators into one indicator:
    zero_los_and_medicine_related_episodes |>
      dplyr::bind_rows(
        end_of_life_episodes,
        readmission_within_28_days_episodes,
        ambulatory_care_conditions_acute_episodes,
        ambulatory_care_conditions_chronic_episodes,
        frailty_episodes_with_risk_scores
      ) |>
      dplyr::select(
        apce_ident,
        der_postcode_lsoa_2011_code,
        gp_practice_sus,
        date,
        sex,
        age_range,
        spelldur
      ) |>
      unique() # To account for potential overlap between the subindicators
  ),
  tar_target(
    redirection_lsoa,
    redirection_episodes |>
      get_indicator_at_sub_geography_level_by_age_sex("lsoa") |>
      recode_lsoa11_as_lsoa21(lsoa11_to_lsoa_21, "admissions") |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies) |>
      dplyr::mutate(imd_year = lubridate::ymd(date, truncated = 1)) |>
      get_imd_from_lsoa(imd_lsoa_lookup,
                        earliest_imd_year,
                        latest_available_imd)
  ),
  tar_target(
    redirection_gp,
    redirection_episodes |>
      unique() |>
      dplyr::mutate(imd_year = lubridate::ymd(date, truncated = 1)) |>
      get_imd_from_lsoa(imd_lsoa_lookup,
                        earliest_imd_year,
                        latest_available_imd) |>
      dplyr::summarise(
        admissions = dplyr::n(),
        beddays = sum(spelldur, na.rm = TRUE),
        .by = c(date, age_range, sex, imd_quintile, gp_practice_sus)
      ) |>
      join_to_geography_lookup("pcn", gp_to_pcn) |>
      get_nh_from_pcn(pcn_to_nh)
  ),

  # ICB and LA
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      redirection,
      aggregate_indicator_to_geography_level_by_age_sex_imd(
        redirection_lsoa,
        geography,
        "redirection")
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      redirection_indicator_icb,
      get_indicators_age_sex_imd_standardised_rates(
        data = redirection_icb,
        population = population_by_age_sex_imd_icb,
        geography = "icb",
        latest_population_year,
        activity_type,
        standard_pop_by_age_sex_imd
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      redirection_indicator_la,
      get_indicators_age_sex_imd_standardised_rates(
        data = redirection_la,
        population = population_by_age_sex_imd_la,
        geography = "la",
        latest_population_year,
        activity_type,
        standard_pop_by_age_sex_imd
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),
  # NH
  tar_target(
    redirection_nh,
    aggregate_indicator_to_geography_level_by_age_sex_imd(redirection_gp,
                                                          "nh",
                                                          "redirection")
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      redirection_indicator_nh,
      get_indicators_age_sex_imd_standardised_rates(
        data = redirection_nh,
        population = population_by_age_sex_imd_nh,
        geography = "nh",
        latest_population_year,
        activity_type,
        standard_pop_by_age_sex_imd
      ) |>
        dplyr::mutate(frequency = "monthly")
    )
  ),

  ## Available beds data, categorised and distributed --------------------------
  tar_target(
    beds_available_data,
    get_kh03_data(con, current_financial_year_start)
    ),

  tar_target(
    bed_split_icb,
    assign_kh03_beds_icb(beds_available_data,
                         prov_site_type,
                         prov_act_dist_icb) |>
      group_by(icb24cd, der_financial_year) |>
      mutate(year_tot = sum(beds), perc = beds / year_tot * 100) |>
      ungroup() |>
      filter(org_class == 'Acute') |>
      mutate(indicator = 'acute_bedshare_percent') |>
      select(9, 2, 3, 4, 6:8) |>
      dplyr::rename(
        icb = icb24cdh,
        date = der_financial_year,
        numerator = beds,
        denominator = year_tot,
        value = perc
      ) |>
      arrange(icb, date) |>
      dplyr::mutate(
        frequency = "fin_yearly",
        date = glue::glue("{stringr::str_sub(date, 1, 4)}-04")
      )
  ),
  tar_target(
    bed_split_la,
    assign_kh03_beds_lad(beds_available_data,
                         prov_site_type,
                         prov_act_dist_lad) |>
      group_by(lad24cd, der_financial_year) |>
      mutate(year_tot = sum(beds), perc = beds / year_tot * 100) |>
      ungroup() |>
      filter(org_class == 'Acute') |>
      mutate(indicator = 'acute_bedshare_percent') |>
      select(8, 1:3, 5:7) |>
      dplyr::rename(
        la = lad24cd,
        date = der_financial_year,
        numerator = beds,
        denominator = year_tot,
        value = perc
      ) |>
      arrange(la, date) |>
      dplyr::mutate(
        frequency = "fin_yearly",
        date = glue::glue("{stringr::str_sub(date, 1, 4)}-04")
      )
  ),
  tar_target(
    bed_split_nh,
    assign_kh03_beds_nh(beds_available_data,
                         prov_site_type,
                         prov_act_dist_nh) |>
      group_by(nnhip_code, der_financial_year) |>
      mutate(year_tot = sum(beds), perc = beds / year_tot * 100) |>
      ungroup() |>
      filter(org_class == 'Acute') |>
      mutate(indicator = 'acute_bedshare_percent') |>
      select(7, 1, 2, 4:6) |>
      dplyr::rename(
        nh = nnhip_code,
        date = der_financial_year,
        numerator = beds,
        denominator = year_tot,
        value = perc
      ) |>
      arrange(nh, date) |>
      dplyr::mutate(
        frequency = "fin_yearly",
        date = glue::glue("{stringr::str_sub(date, 1, 4)}-04")
      )
  ),

  ## NHS workforce data, categorised and distributed ---------------------------
  tar_target(
    workforce_data,
    get_workforce_data(con, current_financial_year_start)
    ),

  # icb
  tar_target(
    workforce_acute_icb,
    assign_workforce_icb(workforce_data, prov_pat_dist_icb)
  ),

  # lad
  tar_target(
    workforce_acute_lad,
    assign_workforce_lad(workforce_data, prov_pat_dist_lad)
  ),

  # NH
  tar_target(
    workforce_acute_nh,
    assign_workforce_nh(workforce_data, prov_pat_dist_nh, pcn_to_nh)
  ),

  ## Delayed discharge days as % of all bed days -------------------------------
  tar_target(
    del_dis_days,
    get_delay_disch_data(con, admissions_lag_date)),

  # ICB
  tar_target(
    delayed_discharge_percent_icb_beddays,
    del_dis_days |>
      filter(!is.na(lsoa_2011)) |>
      left_join(
        lsoa11_to_lsoa_21 |>
          select(1:2),
        by = c("lsoa_2011" = "lsoa11cd"),
        relationship =
          "many-to-many"
      ) |>
      left_join(
        lsoa_to_higher_geographies |>
          select(1, 6:8),
        by = "lsoa21cd",
        relationship =
          "many-to-many"
      ) |>
      group_by(icb24cd, icb24cdh, icb24nm, year_mon) |>
      summarise(
        spells = sum(spells),
        spell_los = sum(spell_los_tot),
        ddd = sum(ddd_tot)
      ) |>
      filter(!is.na(icb24cd)) |>
      mutate(perc = round(ddd / spell_los * 100, 2), ) |>
      group_by(icb24cd, icb24cdh, icb24nm, year_mon, spells) |>
      PHEindicatormethods::phe_proportion(
        x = ddd,
        n = spell_los,
        confidence = 0.95,
        multiplier = 100
      ) |>
      ungroup() |>
      mutate(indicator = 'delayed_discharge_percent_beddays') |>
      select(14, 2, 4, 6:10) |>
      dplyr::rename(
        icb = icb24cdh,
        date = year_mon,
        numerator = ddd,
        denominator = spell_los
      ) |>
      arrange(icb, date) |>
      dplyr::mutate(frequency = "monthly")
  ),

  # LAD
  tar_target(
    delayed_discharge_percent_la_beddays,
    del_dis_days |>
      filter(!is.na(lsoa_2011)) |>
      left_join(
        lsoa11_to_lsoa_21 |>
          select(1:2),
        by = c("lsoa_2011" = "lsoa11cd"),
        relationship =
          "many-to-many"
      ) |>
      left_join(
        lsoa_to_higher_geographies |>
          select(1, 11:12),
        by = "lsoa21cd",
        relationship =
          "many-to-many"
      ) |>
      group_by(lad24cd, lad24nm, year_mon) |>
      summarise(
        spells = sum(spells),
        spell_los = sum(spell_los_tot),
        ddd = sum(ddd_tot)
      ) |>
      filter(!is.na(lad24cd)) |>
      mutate(perc = round(ddd / spell_los * 100, 2), ) |>
      group_by(lad24cd, lad24nm, year_mon, spells) |>
      PHEindicatormethods::phe_proportion(
        x = ddd,
        n = spell_los,
        confidence = 0.95,
        multiplier = 100
      ) |>
      ungroup() |>
      mutate(indicator = 'delayed_discharge_percent_beddays') |>
      select(
        indicator,
        year_mon,
        lad24cd,
        ddd,
        spell_los,
        value,
        lowercl,
        uppercl
      ) |>
      dplyr::rename(
        la = lad24cd,
        date = year_mon,
        numerator = ddd,
        denominator = spell_los
      ) |>
      arrange(la, date) |>
      dplyr::mutate(frequency = "monthly")
  ),

  # NH
  tar_target(
    delayed_discharge_percent_nh_beddays,
    del_dis_days |>
      filter(!is.na(gp_prac)) |>
      left_join(
        gp_to_pcn |>
          select(1:2, 5:6),
        by = c("gp_prac" = "partner_organisation_code"),
        relationship =
          "many-to-many"
      ) |>
      get_nh_from_pcn(pcn_to_nh) |>
      group_by(nnhip_code, year_mon) |>
      summarise(
        spells = sum(spells),
        spell_los = sum(spell_los_tot),
        ddd = sum(ddd_tot)
      ) |>
      filter(!is.na(nnhip_code), spell_los > 0, ddd <= spell_los) |>
      mutate(perc = round(ddd / spell_los * 100, 2), ) |>
      group_by(nnhip_code, year_mon, spells) |>
      PHEindicatormethods::phe_proportion(
        x = ddd,
        n = spell_los,
        confidence = 0.95,
        multiplier = 100
      ) |>
      ungroup() |>
      mutate(indicator = 'delayed_discharge_percent_beddays') |>
      select(
        indicator,
        year_mon,
        nnhip_code,
        ddd,
        spell_los,
        value,
        lowercl,
        uppercl
      ) |>
      dplyr::rename(
        nh = nnhip_code,
        date = year_mon,
        numerator = ddd,
        denominator = spell_los
      ) |>
      arrange(nh, date) |>
      dplyr::mutate(frequency = "monthly")
  ),

  ## Cost data, community to acute ratio providers re-distributed --------------
  tar_target(
    ncc_cost_data,
    get_cost_data(con, current_financial_year_start)
    ),

  # icb
  tar_target(
    costs_community_ratio_icb,
    assign_costs_icb(ncc_cost_data, prov_pat_dist_lsoa)
  ),
  # lad
  tar_target(
    costs_community_ratio_la,
    assign_costs_lad(ncc_cost_data, prov_pat_dist_lsoa)
  ),
  # NH
  tar_target(
    costs_community_ratio_nh,
    assign_costs_nh(ncc_cost_data, gp_to_pcn, prov_pat_dist_prac, pcn_to_nh)
  ),

  ## Bed split between acute and community provider sites ----------------------
  tar_target(
    bedday_split_data_lsoa,
    get_epi_bedday_data_lsoa(con, start_date, admissions_lag_date)),
  tar_target(
    bedday_split_data_prac,
    get_epi_bedday_data_prac(con, start_date, admissions_lag_date)),

  # icb
  tar_target(
    beddays_split_icb,
    beddays_to_icb(bedday_split_data_lsoa,prov_site_type,lsoa11_to_lsoa_21,lsoa_to_higher_geographies)
    ),
  # lad
  tar_target(
    beddays_split_la,
    beddays_to_lad(bedday_split_data_lsoa, prov_site_type, lsoa11_to_lsoa_21, lsoa_to_higher_geographies)
  ),
  # NH
  tar_target(
    beddays_split_nh,
    beddays_to_nh(bedday_split_data_prac, prov_site_type, gp_to_pcn, pcn_to_nh)
  ),

  ## Emergency admissions with zero length of stay and no procedures -----------
  tar_target(nostaynoproc_data_lsoa,
             get_nostaynoproc_data_lsoa(con,
                                        start_date,
                                        admissions_lag_date)),
  tar_target(nostaynoproc_data_prac,
             get_nostaynoproc_data_prac(con,
                                        start_date,
                                        admissions_lag_date)),

  #icb
  tar_target(
    zerolos_noproc_icb,
    zero_los_no_proc_icb(
      nostaynoproc_data_lsoa,
      lsoa11_to_lsoa_21,
      lsoa_to_higher_geographies,
      population_icb,
      latest_population_year
    )
  ), 
  #lad
  tar_target(
    zerolos_noproc_la,
    zero_los_no_proc_la(
      nostaynoproc_data_lsoa,
      lsoa11_to_lsoa_21,
      lsoa_to_higher_geographies,
      population_la,
      latest_population_year
    )
  ), 
  tar_target(
    zerolos_noproc_nh,
    zero_los_no_proc_nh(nostaynoproc_data_prac,gp_to_pcn,population_nh, pcn_to_nh)
  ),

  ## Virtual ward suitable (emergency) admissions for ARI ----------------------
  tar_target(
    vir_ward_ari_data,
    get_vir_ward_data(con,start_date,admissions_lag_date)
  ),

  #icb
  tar_target(
    vir_ward_ari_beddays_icb,
    vir_ward_ari_icb(
      vir_ward_ari_data,
      lsoa11_to_lsoa_21,
      lsoa_to_higher_geographies,
      population_icb,
      latest_population_year
    )
  ), 

  #lad
  tar_target(
    vir_ward_ari_beddays_la,
    vir_ward_ari_la(
      vir_ward_ari_data,
      lsoa11_to_lsoa_21,
      lsoa_to_higher_geographies,
      population_la,
      latest_population_year
    )
  ), 

  #NH
  tar_target(
    vir_ward_ari_beddays_nh,
    vir_ward_ari_nh(vir_ward_ari_data,gp_to_pcn,population_nh, pcn_to_nh)
  ),
  
  ## Community services contacts per 100,000 population ------------------------
  tar_target(csds_contacts_data,
             get_csds_contacts_data(con,start_date,admissions_lag_date)
             ),
  
  #icb
  tar_target(
    comm_contacts_icb,
    comm_contacts_assign_icb(
      csds_contacts_data,
      lsoa11_to_lsoa_21,
      lsoa_to_higher_geographies,
      population_icb,
      latest_population_year
    )
  ), 
  #la
  tar_target(
    comm_contacts_la,
    comm_contacts_assign_la(
      csds_contacts_data,
      lsoa11_to_lsoa_21,
      lsoa_to_higher_geographies,
      population_la,
      latest_population_year
    )
  ), 
  #pcn
  tar_target(
    comm_contacts_nh,
    comm_contacts_assign_nh(csds_contacts_data,gp_to_pcn,population_nh,pcn_to_nh)
  ),

  # All indicators -------------------------------------------------------------
  tar_target(
    indicators_icb,
    dplyr::bind_rows(
      #elective_non_elective_ratio_icb_admissions,
      elective_non_elective_ratio_icb_beddays,
      #frailty_indicators_icb_admissions,
      frailty_indicators_icb_beddays,
      #readmission_indicator_icb_admissions,
      readmission_indicator_icb_beddays,
      #ambulatory_acute_indicator_icb_admissions,
      ambulatory_acute_indicator_icb_beddays,
      #ambulatory_chronic_indicator_icb_admissions,
      ambulatory_chronic_indicator_icb_beddays,
      frequent_attenders_indicator_icb,
      #raid_ae_indicator_icb_admissions,
      raid_ae_indicator_icb_beddays,
      #falls_indicator_icb_admissions,
      falls_indicator_icb_beddays,
      #redirection_indicator_icb_admissions,
      redirection_indicator_icb_beddays,
      delayed_discharge_percent_icb_beddays,
      workforce_acute_icb,
      costs_community_ratio_icb,
      beddays_split_icb,
      zerolos_noproc_icb,
      vir_ward_ari_beddays_icb,
      comm_contacts_icb
    ) |>
      pin_indicators(icb_lookup, "icb", board)
  ),
  tar_target(
    indicators_la,
    dplyr::bind_rows(
      #elective_non_elective_ratio_la_admissions,
      elective_non_elective_ratio_la_beddays,
      #frailty_indicators_la_admissions,
      frailty_indicators_la_beddays,
      #readmission_indicator_la_admissions,
      readmission_indicator_la_beddays,
      #ambulatory_acute_indicator_la_admissions,
      ambulatory_acute_indicator_la_beddays,
      #ambulatory_chronic_indicator_la_admissions,
      ambulatory_chronic_indicator_la_beddays,
      frequent_attenders_indicator_la,
      #raid_ae_indicator_la_admissions,
      raid_ae_indicator_la_beddays,
      #falls_indicator_la_admissions,
      falls_indicator_la_beddays,
      #redirection_indicator_la_admissions,
      redirection_indicator_la_beddays,
      delayed_discharge_percent_la_beddays,
      workforce_acute_lad,
      costs_community_ratio_la,
      beddays_split_la,
      zerolos_noproc_la,
      vir_ward_ari_beddays_la,
      comm_contacts_la
    ) |>
      pin_indicators(la_lookup, "la", board)
  ),
  tar_target(
    indicators_nh,
    dplyr::bind_rows(
      #elective_non_elective_ratio_nh_admissions,
      elective_non_elective_ratio_nh_beddays,
      #frailty_indicators_nh_admissions,
      frailty_indicators_nh_beddays,
      #readmission_indicator_nh_admissions,
      readmission_indicator_nh_beddays,
      #ambulatory_acute_indicator_nh_admissions,
      ambulatory_acute_indicator_nh_beddays,
      #ambulatory_chronic_indicator_nh_admissions,
      ambulatory_chronic_indicator_nh_beddays,
      frequent_attenders_indicator_nh,
      #raid_ae_indicator_nh_admissions,
      raid_ae_indicator_nh_beddays,
      #falls_indicator_nh_admissions,
      falls_indicator_nh_beddays,
      #redirection_indicator_nh_admissions,
      redirection_indicator_nh_beddays,
      delayed_discharge_percent_nh_beddays,
      workforce_acute_nh,
      costs_community_ratio_nh,
      beddays_split_nh,
      zerolos_noproc_nh,
      vir_ward_ari_beddays_nh,
      comm_contacts_nh
    ) |>
      pin_indicators(nh_lookup, "nh", board)
  ),

  # Percentage change ----------------------------------------------------------
  tar_target(perc_change_icb, get_perc_change(indicators_icb, "icb")),
  tar_target(perc_change_la, get_perc_change(indicators_la, "la")),
  tar_target(perc_change_nh, get_perc_change(indicators_nh, "nh")),
  tar_target(
    perc_change,
    rbind(perc_change_icb, perc_change_la, perc_change_nh) |>
      pin_perc_change(board)
  ),

  # Reference ------------------------------------------------------------------
  ## Geography -----------------------------------------------------------------
  tar_target(ref_icb, get_ref_by_geography(icb_lookup, "icb")),
  tar_target(ref_la, get_ref_by_geography(la_lookup, "la")),
  tar_target(ref_nh, get_ref_by_geography(nh_lookup, "nh")),
  tar_target(
    ref_geography,
    pin_ref_geography(ref_icb, ref_la, ref_nh, board)
  ),

  ## Indicators ----------------------------------------------------------------
  tar_target(ref_indicator, pin_ref_indicator(indicators_icb, board)),
  
  ## PCN to NH -----------------------------------------------------------------
  tar_target(ref_pcn_to_nh, pin_ref_pcn_to_nh(pcn_to_nh, gp_to_pcn, board))
)
