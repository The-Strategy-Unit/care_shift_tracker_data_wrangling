#' Functions for the beddays split %'s indicator

#' The total beddays by provider site by month.
#'
#' @param connection The ODBC connection.
#' @param start minimum date for query from target pipeline.
#' @param lag The maximum date for the query from target pipeline.
#'
#' @returns A dataframe with the number of bed days by lsoa and practice.
get_epi_bedday_data_lsoa <- function(connection, start, lag) {
  
  query <- "
  SET NOCOUNT ON;
  
  with cte as
    (
    select left(Provider_Code,3) as prov_code, left([Der_Provider_Site_Code],5) as prov_site_code, [Der_Postcode_LSOA_2011_Code] as lsoa_2011,
    der_financial_year, left([Der_Activity_Month],4) + '-' + right([Der_Activity_Month],2) as der_activity_month,
    convert(varchar(7), Episode_End_Date, 120) AS date,
    case when [Patient_Classification] in ('3','4') then 0.5
		    when [Der_Episode_LoS] = 0 then 0.5
		    when [Der_Episode_LoS] is NULL then 0.5
		    else [Der_Episode_LoS] end as los

    from [Reporting_MESH_APC].[APCE_Core_Monthly_Snapshot]
    where 1=1
      and [Episode_End_Date] >= 'start_date'
      AND Episode_End_Date < 'lag_date' --total time series
      and [Age_on_Admission] >= 65 -- proxy for frail
      and left([Admission_Method],1) in ('1') --elective only
      and [Patient_Classification] in ('1','2','3') -- exclude regular night and mother & baby
      and [Episode_End_Date] is not NULL -- FCE's only
      and Der_Postcode_LSOA_2011_Code is not NULL
    )

    select prov_code, prov_site_code, lsoa_2011, der_financial_year, der_activity_month,
    sum(los) as sum_los
    from cte
    group by prov_code, prov_site_code, lsoa_2011, der_financial_year, [Der_Activity_Month]
    order by prov_code, prov_site_code, lsoa_2011, der_financial_year, [Der_Activity_Month]
  " |>
    stringr::str_replace_all(
      c("start_date" = start,
        "lag_date" = lag)
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}


get_epi_bedday_data_prac <- function(connection, start, lag) {
  
  query <- "
  SET NOCOUNT ON;
  
  with cte as
    (
    select left(Provider_Code,3) as prov_code, left([Der_Provider_Site_Code],5) as prov_site_code, [GP_Practice_Code] as gp_prac,
    der_financial_year, left([Der_Activity_Month],4) + '-' + right([Der_Activity_Month],2) as der_activity_month,
    convert(varchar(7), Episode_End_Date, 120) AS date,
    case when [Patient_Classification] in ('3','4') then 0.5
		    when [Der_Episode_LoS] = 0 then 0.5
		    when [Der_Episode_LoS] is NULL then 0.5
		    else [Der_Episode_LoS] end as los

    from [Reporting_MESH_APC].[APCE_Core_Monthly_Snapshot]
    where 1=1
      and [Episode_End_Date] >= 'start_date'
      AND Episode_End_Date < 'lag_date' --total time series
      and [Age_on_Admission] >= 65 -- proxy for frail
      and left([Admission_Method],1) in ('1') --elective only
      and [Patient_Classification] in ('1','2','3') -- exclude regular night and mother & baby
      and [Episode_End_Date] is not NULL -- FCE's only
      and GP_Practice_Code is not NULL
    )

    select prov_code, prov_site_code, gp_prac, der_financial_year, der_activity_month,
    sum(los) as sum_los
    from cte
    group by prov_code, prov_site_code, gp_prac, der_financial_year, [Der_Activity_Month]
    order by prov_code, prov_site_code, gp_prac, der_financial_year, [Der_Activity_Month]
  " |>
    stringr::str_replace_all(
      c("start_date" = start,
        "lag_date" = lag)
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

##' function to distribute by ICB
#' @param beddays The beddays data by lsoa
#' @param sites The ERIC reference data
#' @param geog1 The lsoa matching file
#' @param geog2 The lsoa21 to icb ref file 

beddays_to_icb <- function(beddays,sites,geog1,geog2) {

provs <- sites |>
  group_by(organisation_code, org_class, der_financial_year) |>
  summarise(freq = n())

df <- beddays |>
  filter(!is.na(lsoa_2011)) |>
  left_join(geog1 |> select(1,2),
            by = c("lsoa_2011" = "lsoa11cd"),
            relationship = "many-to-many") |>
  filter(!is.na(lsoa21cd)) |>
  left_join(geog2 |> select(1,6,7,8), by = "lsoa21cd",
            relationship = "many-to-many") |>
  filter(!is.na(icb24cd)) |>
  group_by(prov_code, prov_site_code, der_financial_year, der_activity_month,
           icb24cd, icb24cdh, icb24nm) |>
  summarise(sum_los = sum(sum_los)) |>
  ungroup() |>
  left_join(sites |> select(4,6,8),
            by = c("prov_site_code" = "site_code", "der_financial_year" = "der_financial_year")) |>
  left_join(provs, by = c("prov_code" = "organisation_code", "der_financial_year" = "der_financial_year")) |>
  mutate(final_class = case_when(is.na(site_class) ~ org_class,
                                 site_class == 'Multi' & org_class != 'Acute' ~ org_class,
                                 site_class == 'Other' & is.na(org_class) ~ 'Other',
                                 site_class == 'Other' & org_class != 'Acute' ~ org_class,
                                 TRUE ~ site_class))

all <- df |>
  group_by(icb24cdh, der_activity_month) |>
  summarise(count = n()) |>
  ungroup() |>
  select (1,2)

acute <- df |>
  filter(final_class == 'Acute') |>
  group_by(icb24cdh, der_activity_month) |>
  summarise(acute_los = sum(sum_los)) |>
  ungroup()

nonacute <- df |>
  filter(final_class == 'Community' | final_class == 'MHLDA' | final_class == 'Other') |>
  group_by(icb24cdh, der_activity_month) |>
  summarise(nonacute_los = sum(sum_los)) |>
  ungroup()

wrangled <- all |>
  left_join(acute, by = c("icb24cdh" , "der_activity_month")) |>
  left_join(nonacute, by = c("icb24cdh" , "der_activity_month")) |>
  mutate(acute_los = case_when(is.na(acute_los) ~ 0,
                               TRUE ~ acute_los),
         nonacute_los = case_when(is.na(nonacute_los) ~ 0,
                                  TRUE ~ nonacute_los),
         total_los = acute_los + nonacute_los) |>
  phe_proportion(x = nonacute_los,
                 n = total_los,
                 multiplier = 100) |>
  
  dplyr::mutate(dplyr::across(c(value, lowercl, uppercl),
                              ~janitor::round_half_up(.)),
                indicator = "beddays_nonacute_percent") |>
  dplyr::rename(
    icb = icb24cdh,
    date = der_activity_month,
    numerator = nonacute_los,
    denominator = total_los,) |>
  dplyr::select(
    indicator,
    date,
    icb,
    numerator,
    denominator,
    value,
    lowercl,
    uppercl)  |>
  dplyr::mutate(frequency = "monthly") |>
  arrange(icb, date)

return(wrangled)
}

##' function to distribute by LAD
#' @param beddays The beddays data by lsoa
#' @param sites The ERIC reference data
#' @param geog1 The lsoa matching file
#' @param geog2 The lsoa21 to icb ref file 

beddays_to_lad <- function(beddays,sites,geog1,geog2) {
  
  provs <- sites |>
    group_by(organisation_code, org_class, der_financial_year) |>
    summarise(freq = n())
  
  df <- beddays |>
    filter(!is.na(lsoa_2011)) |>
    left_join(geog1 |> select(1,2),
              by = c("lsoa_2011" = "lsoa11cd"),
              relationship = "many-to-many") |>
    filter(!is.na(lsoa21cd)) |>
    left_join(geog2 |> select(1,11,12), by = "lsoa21cd",
              relationship = "many-to-many") |>
    filter(!is.na(lad24cd)) |>
    group_by(prov_code, prov_site_code, der_financial_year, der_activity_month,
             lad24cd, lad24nm) |>
    summarise(sum_los = sum(sum_los)) |>
    ungroup() |>
    left_join(sites |> select(4,6,8),
              by = c("prov_site_code" = "site_code", "der_financial_year" = "der_financial_year")) |>
    left_join(provs, by = c("prov_code" = "organisation_code", "der_financial_year" = "der_financial_year")) |>
    mutate(final_class = case_when(is.na(site_class) ~ org_class,
                                   site_class == 'Multi' & org_class != 'Acute' ~ org_class,
                                   site_class == 'Other' & is.na(org_class) ~ 'Other',
                                   site_class == 'Other' & org_class != 'Acute' ~ org_class,
                                   TRUE ~ site_class))
  
  all <- df |>
    group_by(lad24cd, der_activity_month) |>
    summarise(count = n()) |>
    ungroup() |>
    select (1,2)
  
  acute <- df |>
    filter(final_class == 'Acute') |>
    group_by(lad24cd, der_activity_month) |>
    summarise(acute_los = sum(sum_los)) |>
    ungroup()
  
  nonacute <- df |>
    filter(final_class == 'Community' | final_class == 'MHLDA' | final_class == 'Other') |>
    group_by(lad24cd, der_activity_month) |>
    summarise(nonacute_los = sum(sum_los)) |>
    ungroup()
  
  wrangled <- all |>
    left_join(acute, by = c("lad24cd" , "der_activity_month")) |>
    left_join(nonacute, by = c("lad24cd" , "der_activity_month")) |>
    mutate(acute_los = case_when(is.na(acute_los) ~ 0,
                                 TRUE ~ acute_los),
           nonacute_los = case_when(is.na(nonacute_los) ~ 0,
                                    TRUE ~ nonacute_los),
           total_los = acute_los + nonacute_los) |>
    phe_proportion(x = nonacute_los,
                   n = total_los,
                   multiplier = 100) |>
    
    dplyr::mutate(dplyr::across(c(value, lowercl, uppercl),
                                ~janitor::round_half_up(.)),
                  indicator = "beddays_nonacute_percent") |>
    dplyr::rename(
      la = lad24cd,
      date = der_activity_month,
      numerator = nonacute_los,
      denominator = total_los,) |>
    dplyr::select(
      indicator,
      date,
      la,
      numerator,
      denominator,
      value,
      lowercl,
      uppercl)  |>
    dplyr::mutate(frequency = "monthly") |>
    arrange(la, date)
  
  return(wrangled)
}

##' function to distribute by PCN
#' @param beddays The beddays data by lsoa
#' @param sites The ERIC reference data
#' @param geog The practice to pcn mapping 

beddays_to_pcn <- function(beddays,sites,geog) {
  
  provs <- sites |>
    group_by(organisation_code, org_class, der_financial_year) |>
    summarise(freq = n())
  
  df <- beddays |>
    filter(!is.na(gp_prac)) |>
    left_join(geog |> select(1,2,5,6),
              by = c("gp_prac" = "partner_organisation_code"),
              relationship = "many-to-many") |>
    filter(!is.na(gp_prac)) |>
    filter(!is.na(pcn_code)) |>
    group_by(prov_code, prov_site_code, der_financial_year, der_activity_month,
             pcn_code, pcn_name) |>
    summarise(sum_los = sum(sum_los)) |>
    ungroup() |>
    left_join(sites |> select(4,6,8),
              by = c("prov_site_code" = "site_code", "der_financial_year" = "der_financial_year")) |>
    left_join(provs, by = c("prov_code" = "organisation_code", "der_financial_year" = "der_financial_year")) |>
    mutate(final_class = case_when(is.na(site_class) ~ org_class,
                                   site_class == 'Multi' & org_class != 'Acute' ~ org_class,
                                   site_class == 'Other' & is.na(org_class) ~ 'Other',
                                   site_class == 'Other' & org_class != 'Acute' ~ org_class,
                                   TRUE ~ site_class))
  
  all <- df |>
    group_by(pcn_code, der_activity_month) |>
    summarise(count = n()) |>
    ungroup() |>
    select (1,2)
  
  acute <- df |>
    filter(final_class == 'Acute') |>
    group_by(pcn_code, der_activity_month) |>
    summarise(acute_los = sum(sum_los)) |>
    ungroup()
  
  nonacute <- df |>
    filter(final_class == 'Community' | final_class == 'MHLDA' | final_class == 'Other') |>
    group_by(pcn_code, der_activity_month) |>
    summarise(nonacute_los = sum(sum_los)) |>
    ungroup()
  
  wrangled <- all |>
    left_join(acute, by = c("pcn_code" , "der_activity_month")) |>
    left_join(nonacute, by = c("pcn_code" , "der_activity_month")) |>
    mutate(acute_los = case_when(is.na(acute_los) ~ 0,
                                    TRUE ~ acute_los),
           nonacute_los = case_when(is.na(nonacute_los) ~ 0,
                                    TRUE ~ nonacute_los),
           total_los = acute_los + nonacute_los) |>
    filter(total_los > 0) |>
    phe_proportion(x = nonacute_los,
                   n = total_los,
                   multiplier = 100) |>
    
    dplyr::mutate(dplyr::across(c(value, lowercl, uppercl),
                                ~janitor::round_half_up(.)),
                  indicator = "beddays_nonacute_percent") |>
    dplyr::rename(
      pcn = pcn_code,
      date = der_activity_month,
      numerator = nonacute_los,
      denominator = total_los,) |>
    dplyr::select(
      indicator,
      date,
      pcn,
      numerator,
      denominator,
      value,
      lowercl,
      uppercl)  |>
    dplyr::mutate(frequency = "monthly") |>
    arrange(pcn, date)
  
  return(wrangled)
}