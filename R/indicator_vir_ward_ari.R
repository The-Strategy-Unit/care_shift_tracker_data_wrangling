#' Functions for the beddays split %'s indicator

#' The total beddays by provider site by month.
#'
#' @param connection The ODBC connection.
#' @param start minimum date for query from target pipeline.
#' @param lag The maximum date for the query from target pipeline.
#'
#' @returns A dataframe with the number of admissions and bed days by lsoa and practice.
get_vir_ward_data <- function(connection, start, lag) {
  
  query <- "
  SET NOCOUNT ON;
  
  with cte as
    (
    select a.der_financial_year, left(a.[Der_Activity_Month],4)+'-'+right(a.[Der_Activity_Month],2) as der_activity_month, a.[Der_Postcode_LSOA_2011_Code] as lsoa_11, a.[GP_Practice_Code] as gp_prac,
    count(*) as spells, sum([Der_Spell_LoS]) as los
    
    from [Reporting_MESH_APC].[APCS_Core_Monthly_Snapshot] a
    left join [Reporting_MESH_APC].[APCS_2526_Der_Monthly_Snapshot] b
    on a.apcs_ident = b.apcs_ident
    
    where 1=1
    and discharge_date between 'start_date' AND 'lag_date' --period of coverage
    and left(admission_method,1) = '2' --unplanned
    and discharge_method in ('1','2','3') --exclude died and stillborn
    and [Age_At_Start_of_Spell_SUS] >= 65 --proxy for frailty
    and (
	    left(b.spell_primary_diagnosis,3) in ('B33','B34','B97',
	    'J06','J07','J08','J09','U04','U06','U07')
	    OR
	    left(b.spell_primary_diagnosis,2) in ('J1','J2','J3',
	    'J4','J5','J6','J7','J8','J9')
	      ) -- acute respiratory infection codes

    group by a.der_financial_year, a.[Der_Activity_Month], a.[Der_Postcode_LSOA_2011_Code], a.[GP_Practice_Code]
    )
    
    select * from cte
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
#' @param data The data target object
#' @param lookup The 2011 to 21 lsoa reference data
#' @param geog The lsoa to higher geography lookup
#' @param pop The 65 and over population target object
#'
#' @returns A dataframe with the rate of bed days per 100,000 population.

vir_ward_ari_icb <- function(data,lookup,geog,pop) {

grouped <- data |>
  group_by(der_financial_year, der_activity_month, lsoa_11) |>
  summarise(los = sum(los)) |>
  ungroup()

df <- grouped |>
  filter(!is.na(lsoa_11)) |>
  left_join(lookup |> select(1,2),
            by = c("lsoa_11" = "lsoa11cd"),
            relationship = "many-to-many") |>
  filter(!is.na(lsoa21cd)) |>
  left_join(geog |> select(1,6,7,8), by = "lsoa21cd",
            relationship = "many-to-many") |>
  filter(!is.na(icb24cd)) |>
  group_by(der_financial_year, der_activity_month, icb24cd, icb24cdh, icb24nm) |>
  summarise(sum_los = sum(los)) |>
  ungroup() |>
  left_join(pop,
            by = c("icb24cdh" = "icb24cdh", "der_financial_year" = "der_financial_year")) |>
  PHEindicatormethods::phe_rate(x = sum_los,
                                n = population,
                                multiplier = 100000) |>
  
  dplyr::mutate(dplyr::across(c(value, lowercl, uppercl),
                              ~janitor::round_half_up(.)),
                indicator = "virtual_ward_ari_bedday_rate") |>
  dplyr::rename(
    icb = icb24cdh,
    date = der_activity_month) |>
  dplyr::select(
    indicator,
    date,
    icb,
    numerator = sum_los,
    denominator = population,
    value,
    lowercl,
    uppercl)  |>
  dplyr::mutate(frequency = "monthly") |>
  arrange(icb, date)

return(df)
}

#' @param data The data target object
#' @param lookup The 2011 to 21 lsoa reference data
#' @param geog The lsoa to higher geography lookup
#' @param pop The 65 and over population target object
#'
#' @returns A dataframe with the rate of bed days per 100,000 population.

vir_ward_ari_la <- function(data,lookup,geog,pop) {
  
  grouped <- data |>
    group_by(der_financial_year, der_activity_month, lsoa_11) |>
    summarise(los = sum(los)) |>
    ungroup()

  df <- grouped |>
    filter(!is.na(lsoa_11)) |>
    left_join(lookup |> select(1,2),
              by = c("lsoa_11" = "lsoa11cd"),
              relationship = "many-to-many") |>
    filter(!is.na(lsoa21cd)) |>
    left_join(geog |> select(1,11,12), by = "lsoa21cd",
              relationship = "many-to-many") |>
    filter(!is.na(lad24cd)) |>
    group_by(der_financial_year, der_activity_month, lad24cd, lad24nm) |>
    summarise(sum_los = sum(los)) |>
    ungroup() |>
    left_join(pop, by = c("lad24cd" = "lad24cd", "der_financial_year" = "der_financial_year")) |>
    PHEindicatormethods::phe_rate(x = sum_los,
                                  n = population,
                                  multiplier = 100000) |>
    
    dplyr::mutate(dplyr::across(c(value, lowercl, uppercl),
                                ~janitor::round_half_up(.)),
                  indicator = "virtual_ward_ari_bedday_rate") |>
    dplyr::rename(
      la = lad24cd,
      date = der_activity_month) |>
    dplyr::select(
      indicator,
      date,
      la,
      numerator = sum_los,
      denominator = population,
      value,
      lowercl,
      uppercl)  |>
    dplyr::mutate(frequency = "monthly") |>
    arrange(la, date)
  
  return(df)
}

#' ##' function to distribute by PCN
#' #' @param beddays The beddays data by lsoa
#' #' @param sites The ERIC reference data
#' #' @param geog The practice to pcn mapping
#' 
#' beddays_to_pcn <- function(beddays,sites,geog) {
#' 
#'   provs <- sites |>
#'     group_by(organisation_code, org_class, der_financial_year) |>
#'     summarise(freq = n())
#' 
#'   df <- beddays |>
#'     filter(!is.na(gp_prac)) |>
#'     left_join(geog |> select(1,2,5,6),
#'               by = c("gp_prac" = "partner_organisation_code"),
#'               relationship = "many-to-many") |>
#'     filter(!is.na(gp_prac)) |>
#'     filter(!is.na(pcn_code)) |>
#'     group_by(prov_code, prov_site_code, der_financial_year, der_activity_month,
#'              pcn_code, pcn_name) |>
#'     summarise(sum_los = sum(sum_los)) |>
#'     ungroup() |>
#'     left_join(sites |> select(4,6,8),
#'               by = c("prov_site_code" = "site_code", "der_financial_year" = "der_financial_year")) |>
#'     left_join(provs, by = c("prov_code" = "organisation_code", "der_financial_year" = "der_financial_year")) |>
#'     mutate(final_class = case_when(is.na(site_class) ~ org_class,
#'                                    site_class == 'Multi' & org_class != 'Acute' ~ org_class,
#'                                    site_class == 'Other' & is.na(org_class) ~ 'Other',
#'                                    site_class == 'Other' & org_class != 'Acute' ~ org_class,
#'                                    TRUE ~ site_class))
#' 
#'   all <- df |>
#'     group_by(pcn_code, der_activity_month) |>
#'     summarise(count = n()) |>
#'     ungroup() |>
#'     select (1,2)
#' 
#'   acute <- df |>
#'     filter(final_class == 'Acute') |>
#'     group_by(pcn_code, der_activity_month) |>
#'     summarise(acute_los = sum(sum_los)) |>
#'     ungroup()
#' 
#'   nonacute <- df |>
#'     filter(final_class == 'Community' | final_class == 'MHLDA' | final_class == 'Other') |>
#'     group_by(pcn_code, der_activity_month) |>
#'     summarise(nonacute_los = sum(sum_los)) |>
#'     ungroup()
#' 
#'   wrangled <- all |>
#'     left_join(acute, by = c("pcn_code" , "der_activity_month")) |>
#'     left_join(nonacute, by = c("pcn_code" , "der_activity_month")) |>
#'     mutate(acute_los = case_when(is.na(acute_los) ~ 0,
#'                                     TRUE ~ acute_los),
#'            nonacute_los = case_when(is.na(nonacute_los) ~ 0,
#'                                     TRUE ~ nonacute_los),
#'            perc_nonacute = nonacute_los / (acute_los + nonacute_los) * 100)
#' 
#'   return(wrangled)
#' }