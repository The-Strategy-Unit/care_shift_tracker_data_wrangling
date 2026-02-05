#' Functions for the beddays split %'s indicator

#' The total beddays by provider site by month.
#'
#' @param connection The ODBC connection.
#' @param start minimum date for query from target pipeline.
#' @param lag The maximum date for the query from target pipeline.
#'
#' @returns A dataframe with the number of admissions and bed days by lsoa and practice.
get_csds_contacts_data <- function(connection, start, lag) {
  
  query <- "
  SET NOCOUNT ON;
  
    with pre_dup as
      (
      select person_ID, carecontactID, contact_date, contact_time, [CareContact_Duration],
      cast(datepart(year,contact_date) as varchar) + '-' + 
	    case when datepart(month,contact_date) < 10
	    then ('0' + cast(datepart(month,contact_date) as varchar))
	    else cast(datepart(month,contact_date) as varchar)
	    end as der_activity_month,
	    case when datepart(mm,contact_date) > 3
	      then cast(datepart(year, contact_date) as varchar) + '/' + right(cast(datepart(year, contact_date)+1 as varchar),2)
	      else cast(datepart(year, contact_date)-1 as varchar) + '/' + right(cast(datepart(year, contact_date) as varchar),2)
	      end as der_financial_year,
      recordnumber, servicerequestID, uniquesubmissionid,
      row_number() OVER (partition by person_ID, servicerequestID, carecontactID order by uniquesubmissionid DESC) as rownum
      FROM [MESH_CSDS].[CYP201CareContact_2]

      where 1=1
      and [Contact_Date] >= 'start_date'
      AND [Contact_Date] < 'lag_date'
      and ageyr_contact_date >= 65 --proxy for frail
      and contact_cancellationreason is NULL --attended
      and carecontact_duration >= 15 --proxy for clinical contact
      and person_ID is not NULL
      ),

      post_dup as
      (select *
      from pre_dup
      where rownum = 1
      ),

      base as
      (select a.*,
      b.[Der_Postcode_yr2011_LSOA] as lsoa_2011,
      c.[OrgID_GP] as gp_prac
      from post_dup a
      left outer join [MESH_CSDS].[CYP001MPI_1] b
      on a.person_id = b.person_ID
      and a.recordnumber = b.recordnumber
      left outer join [MESH_CSDS].[CYP002GP_2] c
      on a.person_id = c.person_ID
      and a.recordnumber = c.recordnumber
      ),

    final as
    (SELECT distinct *
    from base)

      Select lsoa_2011, gp_prac, der_financial_year, der_activity_month,
      count(distinct carecontactID) as contacts
      from final
      group by lsoa_2011, gp_prac, der_financial_year, der_activity_month
      order by lsoa_2011, gp_prac, der_financial_year, der_activity_month
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
#' @returns A dataframe with the rate of community contacts per 100,000 population.

comm_contacts_assign_icb <- function(data,lookup,geog,pop) {

grouped <- data |>
  group_by(der_financial_year, der_activity_month, lsoa_2011) |>
  summarise(contacts = sum(contacts)) |>
  ungroup()

df <- grouped |>
  filter(!is.na(lsoa_2011)) |>
  left_join(lookup |> select(1,2),
            by = c("lsoa_2011" = "lsoa11cd"),
            relationship = "many-to-many") |>
  filter(!is.na(lsoa21cd)) |>
  left_join(geog |> select(1,6,7,8), by = "lsoa21cd",
            relationship = "many-to-many") |>
  filter(!is.na(icb24cd)) |>
  group_by(der_financial_year, der_activity_month, icb24cd, icb24cdh, icb24nm) |>
  summarise(sum_contacts = sum(contacts)) |>
  ungroup() |>
  left_join(pop,
            by = c("icb24cdh" = "icb", "der_financial_year")) |>
  PHEindicatormethods::phe_rate(x = sum_contacts,
                                n = population_size,
                                multiplier = 100000) |>

  dplyr::mutate(dplyr::across(c(value, lowercl, uppercl),
                              ~janitor::round_half_up(.)),
                indicator = "community_services_contacts_per_pop") |>
  dplyr::rename(
    icb = icb24cdh,
    date = der_activity_month) |>
  dplyr::select(
    indicator,
    date,
    icb,
    numerator = sum_contacts,
    denominator = population_size,
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
#' @returns A dataframe with the rate of community contacts per 100,000 population.

comm_contacts_assign_la <- function(data,lookup,geog,pop) {

  grouped <- data |>
    group_by(der_financial_year, der_activity_month, lsoa_2011) |>
    summarise(contacts = sum(contacts)) |>
    ungroup()

  df <- grouped |>
    filter(!is.na(lsoa_2011)) |>
    left_join(lookup |> select(1,2),
              by = c("lsoa_2011" = "lsoa11cd"),
              relationship = "many-to-many") |>
    filter(!is.na(lsoa21cd)) |>
    left_join(geog |> select(1,11,12), by = "lsoa21cd",
              relationship = "many-to-many") |>
    filter(!is.na(lad24cd)) |>
    group_by(der_financial_year, der_activity_month, lad24cd, lad24nm) |>
    summarise(sum_contacts = sum(contacts)) |>
    ungroup() |>
    left_join(pop,
              by = c("lad24cd" = "la", "der_financial_year")) |>
    PHEindicatormethods::phe_rate(x = sum_contacts,
                                  n = population_size,
                                  multiplier = 100000) |>

    dplyr::mutate(dplyr::across(c(value, lowercl, uppercl),
                                ~janitor::round_half_up(.)),
                  indicator = "community_services_contacts_per_pop") |>
    dplyr::rename(
      la = lad24cd,
      date = der_activity_month) |>
    dplyr::select(
      indicator,
      date,
      la,
      numerator = sum_contacts,
      denominator = population_size,
      value,
      lowercl,
      uppercl)  |>
    dplyr::mutate(frequency = "monthly") |>
    arrange(la, date)

  return(df)
}

#' @param data The data target object
#' @param geog The practice to pcn lookup
#' @param pop The 65 and over population target object
#' @param lookup The pcn to NH lookup.
#'
#' @returns A dataframe with the rate of community contacts per 100,000 population.

comm_contacts_assign_nh <- function(data,geog,pop,lookup) {

  grouped <- data |>
    group_by(der_financial_year, der_activity_month, gp_prac) |>
    summarise(contacts = sum(contacts)) |>
    ungroup()

  df <- grouped |>
    filter(!is.na(gp_prac)) |>
    left_join(geog |> select(1,2,5,6),
              by = c("gp_prac" = "partner_organisation_code"),
              relationship = "many-to-many") |>
    filter(!is.na(gp_prac)) |>
    filter(!is.na(pcn_code)) |>
    get_nh_from_pcn(lookup) |>
    group_by(der_financial_year, der_activity_month,
             nnhip_code) |>
    summarise(sum_contacts = sum(contacts)) |>
    ungroup() |>
    left_join(pop, by = c("nnhip_code", "der_activity_month" = "date")) |>
    filter(population_size > 0) |>
    PHEindicatormethods::phe_rate(x = sum_contacts,
                                  n = population_size,
                                  multiplier = 100000) |>

    dplyr::mutate(dplyr::across(c(value, lowercl, uppercl),
                                ~janitor::round_half_up(.)),
                  indicator = "community_services_contacts_per_pop") |>
    dplyr::rename(
      nh = nnhip_code,
      date = der_activity_month) |>
    dplyr::select(
      indicator,
      date,
      nh,
      numerator = sum_contacts,
      denominator = population_size,
      value,
      lowercl,
      uppercl)  |>
    dplyr::mutate(frequency = "monthly") |>
    arrange(nh, date)

  return(df)
}
