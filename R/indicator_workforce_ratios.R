#' Functions for the workforce ratio indicator

#' The number of clinical staff by provider and 
#' year.
#'
#' @param connection The ODBC connection.
#'
#' @returns A dataframe with the number of beds (KH03) by provider and year.

get_workforce_data <- function(connection, financial_year, start_date) {
  
  query <- "
  SET NOCOUNT ON;
  
    select org_code, cluster_group, staff_group, data_type, total, effective_snapshot_date,
    cast(datepart(yyyy, effective_snapshot_date) as varchar) + '/' + cast(right(datepart(yyyy, effective_snapshot_date)+1,2) as varchar) as der_financial_year

    from [UKHF_NHS_Workforce].[Staff_Group_And_Organisation1]
    where 1 = 1
    AND Cluster_Group in ('Acute','Community Provider Trust','Mental Health') --exclude commissioning staff
    AND Data_Type = 'FTE' --better representation than headcount
    AND Staff_Group in ('HCHS doctors','Nurses & health visitors','Midwives','Support to clinical staff','Professionally qualified clinical staff') --different components of clinical staff
    AND [Effective_Snapshot_Date] >= 'start_date' 
    AND effective_snapshot_date < 'fy_date'
    AND datepart(mm, [Effective_Snapshot_Date]) = '06' --end of Q1 as proxy for current financial year
    
    ORDER BY org_code, cluster_group, staff_group, effective_snapshot_date
  " |>
    stringr::str_replace_all(c("fy_date" = financial_year,
                               "start_date" = start_date))
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

assign_workforce_icb <- function(data, dist_geog) {

  wrangled <- data |>
    # only use combined clinical staff numbers
    filter(staff_group == 'Professionally qualified clinical staff') |>
    # join results to activity distributions
    left_join(dist_geog, by = c("org_code" = "prov_code", "der_financial_year" = "der_financial_year")) |>
    mutate(pats_adj = total*prop_pat) |>
    group_by(icb24cdh, icb24nm, cluster_group, der_financial_year) |>
    summarise(pats = round(sum(pats_adj),4)) |>
    ungroup() |>
    filter(!is.na(icb24cdh)) |>
    group_by(icb24cdh, der_financial_year) |>
    mutate(
      indicator = 'workforce_in_acute_setting_percent',
      year_tot = sum(pats)
      ) |>
    ungroup() |>
    phe_proportion(x = pats, n = year_tot, multiplier = 100) |>
    filter(cluster_group == 'Acute') |>
    dplyr::rename(
      icb = icb24cdh,
      date = der_financial_year,
      numerator = pats,
      denominator = year_tot
    ) |>
    
    select(indicator,
           date,
           icb,
           numerator,
           denominator,
           value,
           lowercl,
           uppercl) |>
    arrange(icb, date) |>
    dplyr::mutate(
      frequency = "fin_yearly",
      date = glue::glue("{stringr::str_sub(date, 1, 4)}-04")
    )

  return(wrangled)
}

assign_workforce_lad <- function(data, dist_geog) {
  
  wrangled <- data |>
    # only use combined clinical staff numbers
    filter(staff_group == 'Professionally qualified clinical staff') |>
    # join results to activity distributions
    left_join(dist_geog, by = c("org_code" = "prov_code", "der_financial_year" = "der_financial_year")) |>
    mutate(pats_adj = total*prop_pat) |>
    group_by(lad24cd, lad24nm, cluster_group, der_financial_year) |>
    summarise(pats = round(sum(pats_adj),4)) |>
    ungroup() |>
    filter(!is.na(lad24cd)) |>
    group_by(lad24cd, der_financial_year) |>
    mutate(
      indicator = 'workforce_in_acute_setting_percent',
      year_tot = sum(pats)
    ) |>
    ungroup() |>
    phe_proportion(x = pats, n = year_tot, multiplier = 100) |>
    filter(cluster_group == 'Acute') |>
    dplyr::rename(
      la = lad24cd,
      date = der_financial_year,
      numerator = pats,
      denominator = year_tot
    ) |>
    
    select(indicator,
           date,
           la,
           numerator,
           denominator,
           value,
           lowercl,
           uppercl) |>
    arrange(la, date) |>
    dplyr::mutate(
      frequency = "fin_yearly",
      date = glue::glue("{stringr::str_sub(date, 1, 4)}-04")
    )
}

assign_workforce_nh <- function(data, dist_geog, lookup) {
  
  wrangled <- data |>
    # only use combined clinical staff numbers
    filter(staff_group == 'Professionally qualified clinical staff') |>
    # join results to activity distributions
    left_join(dist_geog, by = c("org_code" = "prov_code", "der_financial_year" = "der_financial_year")) |>
    mutate(pats_adj = total*prop_pat) |>
    group_by(nnhip_code, cluster_group, der_financial_year) |>
    summarise(pats = round(sum(pats_adj),4)) |>
    ungroup() |>
    filter(!is.na(nnhip_code)) |>
    group_by(nnhip_code, der_financial_year) |>
    mutate(
      indicator = 'workforce_in_acute_setting_percent',
      year_tot = sum(pats)
    ) |>
    ungroup() |>
    phe_proportion(x = pats, n = year_tot, multiplier = 100) |>
    filter(cluster_group == 'Acute') |>
    dplyr::rename(
      nh = nnhip_code,
      date = der_financial_year,
      numerator = pats,
      denominator = year_tot
    ) |>
    select(indicator,
           date,
           nh,
           numerator,
           denominator,
           value,
           lowercl,
           uppercl) |>
    arrange(nh, date) |>
    dplyr::mutate(
      frequency = "fin_yearly",
      date = glue::glue("{stringr::str_sub(date, 1, 4)}-04")
    )
}