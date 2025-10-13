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
  
  ## Population by age range and sex -------------------------------------------
  tar_target(
    population_lsoa_by_age_sex,
    get_population_lsoa_by_age_sex(age_cutoff, start_date, con)
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
      tidyr::pivot_longer(dplyr::contains("2021"), 
                          names_to = "sex", 
                          values_to = "pop") |>
      dplyr::mutate(sex = ifelse(stringr::str_detect(sex, "female"),
                                 "2", 
                                 "1"),
                    age_range = age |>
                      stringr::str_replace_all(c("Aged " = "",
                                                 " to " = "-",
                                                 " years" = "",
                                                 "4 and under" = "0-4",
                                                 "90 and over" = "85+",
                                                 "85-89" = "85+"))) |>
      na.omit() |>
      dplyr::summarise(pop = sum(pop),
                       .by = c(age_range, sex))
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
  tar_target(
    frailty_episodes,
    get_frailty_data(start_date, con)
  ),
  # LSOA and GP
  tar_target(
    frailty_episodes_with_risk_scores,
    get_frailty_with_risk_scores(frailty_episodes,
                                 frailty_risk_scores)
  ),
  tarchetypes::tar_map(
    list(sub_geography = c("gp", "lsoa")),
    tar_target(
      frailty,
      get_frailty_sub_geography(frailty_episodes_with_risk_scores,
                                sub_geography)
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
    get_emergency_indicator_episodes(age_cutoff, 
                                     start_date, 
                                     readmissions_where_clause,
                                     con)
  ),
  tar_target(
    readmission_within_28_days_lsoa,
    readmission_within_28_days_episodes |>
      get_indicator_at_sub_geography_level("lsoa") |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies) 
  ),
  tar_target(
    readmission_within_28_days_gp,
    readmission_within_28_days_episodes |>
      get_indicator_at_sub_geography_level("gp") |> 
      join_to_geography_lookup("pcn", gp_to_pcn) 
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      readmission_within_28_days,
      aggregate_indicator_to_geography_level(readmission_within_28_days_lsoa, 
                                             geography,
                                             "readmission_within_28_days")
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
    aggregate_indicator_to_geography_level(readmission_within_28_days_gp, 
                                           "pcn",
                                           "readmission_within_28_days")
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
    get_emergency_indicator_episodes(age_cutoff, 
                                     start_date, 
                                     ambulatory_care_acute_where_clause,
                                     con)
  ),
  tar_target(
    ambulatory_care_conditions_acute_lsoa,
    ambulatory_care_conditions_acute_episodes |>
      get_indicator_at_sub_geography_level("lsoa") |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies) 
  ),
  tar_target(
    ambulatory_care_conditions_acute_gp,
    ambulatory_care_conditions_acute_episodes |>
      get_indicator_at_sub_geography_level("gp") |> 
      join_to_geography_lookup("pcn", gp_to_pcn) 
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      ambulatory_care_conditions_acute,
      aggregate_indicator_to_geography_level(
        ambulatory_care_conditions_acute_lsoa, 
        geography, 
        "ambulatory_care_conditions_acute")
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
  # PCN
  tar_target(
    ambulatory_care_conditions_acute_pcn,
    aggregate_indicator_to_geography_level(
      ambulatory_care_conditions_acute_gp,
      "pcn",
      "ambulatory_care_conditions_acute")
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
    get_emergency_indicator_episodes(age_cutoff, 
                                     start_date, 
                                     ambulatory_care_chronic_where_clause,
                                     con)
  ),
  tar_target(
    ambulatory_care_conditions_chronic_lsoa,
    ambulatory_care_conditions_chronic_episodes |>
      get_indicator_at_sub_geography_level("lsoa") |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies) 
  ),
  tar_target(
    ambulatory_care_conditions_chronic_gp,
    ambulatory_care_conditions_chronic_episodes |>
      get_indicator_at_sub_geography_level("gp") |> 
      join_to_geography_lookup("pcn", gp_to_pcn) 
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      ambulatory_care_conditions_chronic,
      aggregate_indicator_to_geography_level(
        ambulatory_care_conditions_chronic_lsoa, 
        geography, 
        "ambulatory_care_conditions_chronic")
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
    ambulatory_care_conditions_chronic_pcn,
    aggregate_indicator_to_geography_level(
      ambulatory_care_conditions_chronic_gp,
      "pcn",
      "ambulatory_care_conditions_chronic")
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
      ambulatory_care_vaccine_preventable_where_clause,
      con)
  ),
  tar_target(
    ambulatory_care_conditions_vaccine_preventable_lsoa,
    ambulatory_care_conditions_vaccine_preventable_episodes |>
      get_indicator_at_sub_geography_level("lsoa") |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies) 
  ),
  tar_target(
    ambulatory_care_conditions_vaccine_preventable_gp,
    ambulatory_care_conditions_vaccine_preventable_episodes |>
      get_indicator_at_sub_geography_level("gp") |> 
      join_to_geography_lookup("pcn", gp_to_pcn) 
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
  
  ## Mental Health Admissions via ED -------------------------------------------
  # LSOA and GP
  tar_target(
    raid_ae_lsoa,
    get_raid_ae_sub_geography("lsoa", 
                              age_cutoff, 
                              start_date, 
                              con) |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies)
  ),
  tar_target(
    raid_ae_gp,
    get_raid_ae_sub_geography("gp", 
                              age_cutoff, 
                              start_date, 
                              con) |>
      join_to_geography_lookup("pcn", gp_to_pcn)
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      raid_ae,
      aggregate_indicator_to_geography_level(raid_ae_lsoa, 
                                             geography,
                                             "raid_ae")
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
      )
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
      )
    )
  ),
  # PCN
  tar_target(
    raid_ae_pcn,
    aggregate_indicator_to_geography_level(raid_ae_gp, 
                                           "pcn",
                                           "raid_ae")
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      raid_ae_indicator_pcn,
      get_indicators_per_pop(
        raid_ae_pcn,
        population_pcn,
        "pcn",
        latest_population_year,
        activity_type
      )
    )
  ),
  
  ## Emergency hospital admissions due to falls in people over 65 --------------
  tar_target(
    falls_where_clause,
    "(LEFT(Der_Primary_Diagnosis_Code, 4) = 'R296' OR
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
    )
    "
  ),
  # LSOA and GP
  tar_target(
    falls_related_admissions_episodes,
    get_emergency_indicator_episodes(age_cutoff, 
                                     start_date, 
                                     falls_where_clause,
                                     con)
  ),
  tar_target(
    falls_related_admissions_lsoa,
    falls_related_admissions_episodes |>
      get_indicator_at_sub_geography_level("lsoa") |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies) 
  ),
  tar_target(
    falls_related_admissions_gp,
    falls_related_admissions_episodes |>
      get_indicator_at_sub_geography_level("gp") |> 
      join_to_geography_lookup("pcn", gp_to_pcn) 
  ),
  # ICB and LA
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      falls_related_admissions,
      aggregate_indicator_to_geography_level(falls_related_admissions_lsoa, 
                                             geography,
                                             "falls_related_admissions")
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
      )
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
      )
    )
  ),
  # PCN
  tar_target(
    falls_related_admissions_pcn,
    aggregate_indicator_to_geography_level(falls_related_admissions_gp, 
                                           "pcn",
                                           "falls_related_admissions")
  ),
  tarchetypes::tar_map(
    list(activity_type = c("admissions", "beddays")),
    tar_target(
      falls_indicator_pcn,
      get_indicators_per_pop(
        falls_related_admissions_pcn,
        population_pcn,
        "pcn",
        latest_population_year,
        activity_type
      )
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
  tar_target(redirection_where_clause,
             paste(zero_los_no_procedure_where_clause,
                   medicines_related_admissions_where_clause,
                   sep = " AND ")),
  tar_target(
    zero_los_and_medicine_related_episodes,
    get_emergency_indicator_episodes(age_cutoff, 
                                     start_date, 
                                     redirection_where_clause,
                                     con)
  ),
  
  tar_target(
    end_of_life_episodes,
    get_end_of_life_episodes(age_cutoff, start_date, con) 
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
      dplyr::select(apce_ident, 
                    der_postcode_lsoa_2021_code, 
                    gp_practice_sus, 
                    date, 
                    sex,
                    age_range, 
                    spelldur) |>
      unique() 
  ),
  tar_target(
    redirection_lsoa,
    redirection_episodes |>
      get_indicator_at_sub_geography_level_by_age_sex("lsoa") |>
      join_to_geography_lookup("icb", lsoa_to_higher_geographies)
  ),
  tar_target(
    redirection_gp,
    redirection_episodes |>
      get_indicator_at_sub_geography_level_by_age_sex("gp") |>
      join_to_geography_lookup("pcn", gp_to_pcn)
  ),

  # ICB and LA
  tarchetypes::tar_map(
    list(geography = c("icb", "la")),
    tar_target(
      redirection,
      aggregate_indicator_to_geography_level_by_age_sex(redirection_lsoa,
                                                        geography,
                                                        "redirection")
    )
  ),
  # tarchetypes::tar_map(
  #   list(activity_type = c("admissions", "beddays")),
  #   tar_target(
  #     readmission_indicator_icb,
  #     get_indicators_per_pop(
  #       redirection_icb,
  #       population_icb_by_age_sex,
  #       "icb",
  #       latest_population_year,
  #       activity_type
  #     )
  #   )
  # ),
  # tarchetypes::tar_map(
  #   list(activity_type = c("admissions", "beddays")),
  #   tar_target(
  #     readmission_indicator_la,
  #     get_indicators_per_pop(
  #       redirection_la,
  #       population_la_by_age_sex,
  #       "la",
  #       latest_population_year,
  #       activity_type
  #     )
  #   )
  # ),
  # PCN
  tar_target(
    redirection_pcn,
    aggregate_indicator_to_geography_level_by_age_sex(redirection_gp,
                                                      "pcn",
                                                      "redirection")
  ),
  # tarchetypes::tar_map(
  #   list(activity_type = c("admissions", "beddays")),
  #   tar_target(
  #     readmission_indicator_pcn,
  #     get_indicators_per_pop(
  #       redirection_pcn,
  #       population_pcn_by_age_sex,
  #       "pcn",
  #       latest_population_year,
  #       activity_type
  #     )
  #   )
  # ),
  
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
      frequent_attenders_indicator_icb,
      raid_ae_indicator_icb_admissions,
      raid_ae_indicator_icb_beddays,
      falls_indicator_icb_admissions,
      falls_indicator_icb_beddays
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
      frequent_attenders_indicator_la,
      raid_ae_indicator_la_admissions,
      raid_ae_indicator_la_beddays,
      falls_indicator_la_admissions,
      falls_indicator_la_beddays
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
      frequent_attenders_indicator_pcn,
      raid_ae_indicator_pcn_admissions,
      raid_ae_indicator_pcn_beddays,
      falls_indicator_pcn_admissions,
      falls_indicator_pcn_beddays
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
