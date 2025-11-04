#' Functions for the bed ratio indicator

#' The number of available beds (KH03) by provider and 
#' year.
#'
#' @param connection The ODBC connection.
#'
#' @returns A dataframe with the number of beds (KH03) by provider and year.

get_kh03_data <- function(connection) {
  
  query <- "
  SET NOCOUNT ON;
  
    Select organisation_code,
    cast(datepart(yyyy, effective_snapshot_date)-1 as varchar) + '/' + cast(right(datepart(yyyy, effective_snapshot_date),2) as varchar) as der_financial_year,
    sum(number_of_beds) as bed_total
    from [UKHF_Bed_Availability].[Provider_By_Sector_Available_Overnight_Beds1]
    where datepart(mm, effective_snapshot_date) = 03
    and sector in ('Acute','General & Acute','Geriatric')
    group by organisation_code,
    cast(datepart(yyyy, effective_snapshot_date)-1 as varchar) + '/' + cast(right(datepart(yyyy, effective_snapshot_date),2) as varchar)
    order by organisation_code, der_financial_year
  "
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

#' Assigning care setting categories to the above
#' @param data - the raw data captured in above target
#' @param lookup - the ERIC classifications (require grouping for provider level)
#' @param dist - the target with the provider beddays distributed to ICB, LAD or PCN
#' @returns A dataframe with each provider assigned to community, acute or other.

assign_kh03_beds_icb <- function(data, lookup, dist_geog) {
  lookup_trim <- lookup |>
  select(1,3,8) |>
  distinct() |>
  arrange(organisation_code, org_class, der_financial_year)

  wrangled <- data |>
    filter(!is.na(bed_total)) |>
    left_join(lookup_trim, by = c("organisation_code", "der_financial_year")) |>
  # join results to activity distributions
    left_join(dist_geog |>
                select(1:5,9), by = c("organisation_code" = "prov_code", "der_financial_year" = "der_financial_year")) |>
    mutate(beds_adj = bed_total*prop_bed) |>
    group_by(icb24cd, icb24cdh, icb24nm, der_financial_year, org_class) |>
    summarise(beds = round(sum(beds_adj),4)) |>
    filter(!is.na(icb24cd)) |>
    arrange(icb24cd, org_class, der_financial_year)
  
  return(wrangled)
}

assign_kh03_beds_lad <- function(data, lookup, dist_geog) {
  lookup_trim <- lookup |>
    select(1,3,8) |>
    distinct() |>
    arrange(organisation_code, org_class, der_financial_year)
  
  wrangled <- data |>
    filter(!is.na(bed_total)) |>
    left_join(lookup_trim, by = c("organisation_code", "der_financial_year")) |>
    # join results to activity distributions
    left_join(dist_geog |>
                select(1:4,8), by = c("organisation_code" = "prov_code", "der_financial_year" = "der_financial_year")) |>
    mutate(beds_adj = bed_total*prop_bed) |>
    group_by(lad24cd, lad24nm, der_financial_year, org_class) |>
    summarise(beds = round(sum(beds_adj),4)) |>
    filter(!is.na(lad24cd)) |>
    arrange(lad24cd, org_class, der_financial_year)
  
  return(wrangled)
}

assign_kh03_beds_pcn <- function(data, lookup, dist_geog) {
  lookup_trim <- lookup |>
    select(1,3,8) |>
    distinct() |>
    arrange(organisation_code, org_class, der_financial_year)
  
  wrangled <- data |>
    filter(!is.na(bed_total)) |>
    left_join(lookup_trim, by = c("organisation_code", "der_financial_year")) |>
    # join results to activity distributions
    left_join(dist_geog |>
                select(1:4,8), by = c("organisation_code" = "prov_code", "der_financial_year" = "der_financial_year")) |>
    mutate(beds_adj = bed_total*prop_bed) |>
    group_by(pcn_code, pcn_name, der_financial_year, org_class) |>
    summarise(beds = round(sum(beds_adj),4)) |>
    filter(!is.na(pcn_code)) |>
    arrange(pcn_code, org_class, der_financial_year)
  
  return(wrangled)
}