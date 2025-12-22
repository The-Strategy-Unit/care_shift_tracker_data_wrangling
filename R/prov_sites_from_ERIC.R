# Functions to get the community/acute provider lookups from ERIC data.

#' @param connection The ODBC connection
#'
#' @returns A dataframe with the provider and site codes over ERIC reporting
#' periods classified to type of service (community, acute etc...).

get_eric_site_classifications <- function(connection) {
  
  query <- "
  SET NOCOUNT ON;
  
    WITH src AS (
  SELECT [Organisation_Code]
    ,[Organisation_Type]
    ,Case
      when [Organisation_Type] in ('COMMUNITY','COMMUNITY LONDON','COMMUNITY WITH MENTAL HEALTH','LARGE COMMUNITY',
      'LARGE COMMUNITY WITH MENTAL HEALTH','MEDIUM COMMUNITY','MEDIUM COMMUNITY WITH MENTAL HEALTH',
      'SMALL COMMUNITY','SMALL COMMUNITY WITH MENTAL HEALTH', 'CARE TRUST', 'SOCIAL ENTERPRISE',
      'PCT','PRIMARY CARE TRUST') then 'Community'
      when [Organisation_Type] in ('ACUTE - LARGE','ACUTE - MEDIUM','ACUTE - MULTI-SERVICE','ACUTE - SMALL','ACUTE - SPECIALIST',
      'ACUTE - TEACHING','ACUTE SPECIALIST','ACUTE TEACHING LONDON','ACUTE TEACHING OUTSIDE LONDON','LARGE ACUTE LONDON',
      'LARGE ACUTE OUTSIDE LONDON','MEDIUM ACUTE LONDON','MEDIUM ACUTE OUTSIDE LONDON','SMALL ACUTE LONDON','SMALL ACUTE OUTSIDE LONDON',
      'SPECIALIST','TEACHING LONDON','TEACHING OUTSIDE LONDON') then 'Acute'
      when [Organisation_Type] in ('CHILDREN''S SERVICES','CHILDRENS') then 'Children'
      when [Organisation_Type] in ('MENTAL HEALTH','MENTAL HEALTH AND LEARNING DISABILITY','LEARNING DISABILITY') then 'MHLDA'
      when [Organisation_Type] in ('LARGE MULTI-SERVICE','MEDIUM MULTI-SERVICE','MULTI-SERVICE','SMALL MULTI-SERVICE') then 'Multi'
      else 'Other'
      end as Org_Class
    ,[Status]
    ,[Site_Code]
    ,[Site_Type]
    ,Case
      when [Site_Type] in ('7. Community hospital (with inpatient beds)','COMMUNITY HOSPITAL',
      'Community hospital (with inpatient beds)') then 'Community'
      when [Site_Type] in ('1. General acute hospital','2. Specialist hospital (acute only)','GENERAL ACUTE HOSPITAL',
      'Specialist hospital (acute only)') then 'Acute'
      when [Site_Type] in ('4. Mental Health (including Specialist services)','5. Learning Disabilities',
      '6. Mental Health and Learning Disabilities''','Learning Disabilities','Mental Health','Mental Health (including Specialist services)',
      'Mental Health and Learning Disabilities)') then 'MHLDA'
      when [Site_Type] in ('3. Mixed service hospital','Mixed service hospital','MULTI-SERVICE HOSPITAL') then 'Multi'
      else 'Other'
      end as Site_Class
    ,NULL as [Tenure]
    ,NULL as [Non_Inpatient_Type]
    ,NULL as [Type_Of_Lease]
    ,[Measure_Category]
    ,[Measure] as [Measure_Name]
    ,[Measure_Value]
    ,cast([Measure_Value_Str] as nvarchar) as [Measure_Value_Str]
    ,[Effective_Snapshot_Date]
    ,cast(datepart(yyyy,[Effective_Snapshot_Date])-1 as varchar)+'-'+cast(datepart(yyyy,[Effective_Snapshot_Date]) as varchar) as der_fin_year
    ,[DataSourceFileForThisSnapshot_Version]
    ,[Report_Period_Length]
    ,[Unique_ID]
    ,[AuditKey]
FROM [UKHF_Estates_Returns_Information_Collection].[Site_Data1_1]
where [Site_Type] is not NULL
  UNION ALL
  SELECT [Organisation_Code]
    ,[Organisation_Type]
    ,Case
      when [Organisation_Type] in ('COMMUNITY','COMMUNITY LONDON','COMMUNITY WITH MENTAL HEALTH','LARGE COMMUNITY',
      'LARGE COMMUNITY WITH MENTAL HEALTH','MEDIUM COMMUNITY','MEDIUM COMMUNITY WITH MENTAL HEALTH',
      'SMALL COMMUNITY','SMALL COMMUNITY WITH MENTAL HEALTH', 'CARE TRUST', 'SOCIAL ENTERPRISE',
      'PCT','PRIMARY CARE TRUST') then 'Community'
      when [Organisation_Type] in ('ACUTE - LARGE','ACUTE - MEDIUM','ACUTE - MULTI-SERVICE','ACUTE - SMALL','ACUTE - SPECIALIST',
      'ACUTE - TEACHING','ACUTE SPECIALIST','ACUTE TEACHING LONDON','ACUTE TEACHING OUTSIDE LONDON','LARGE ACUTE LONDON',
      'LARGE ACUTE OUTSIDE LONDON','MEDIUM ACUTE LONDON','MEDIUM ACUTE OUTSIDE LONDON','SMALL ACUTE LONDON','SMALL ACUTE OUTSIDE LONDON',
      'SPECIALIST','TEACHING LONDON','TEACHING OUTSIDE LONDON') then 'Acute'
      when [Organisation_Type] in ('CHILDREN''S SERVICES','CHILDRENS') then 'Children'
      when [Organisation_Type] in ('MENTAL HEALTH','MENTAL HEALTH AND LEARNING DISABILITY','LEARNING DISABILITY') then 'MHLDA'
      when [Organisation_Type] in ('LARGE MULTI-SERVICE','MEDIUM MULTI-SERVICE','MULTI-SERVICE','SMALL MULTI-SERVICE') then 'Multi'
      else 'Other'
      end as Org_Class
    ,[Status]
    ,[Site_Code]
    ,[Site_Type]
    ,Case
      when [Site_Type] in ('7. Community hospital (with inpatient beds)','COMMUNITY HOSPITAL',
      'Community hospital (with inpatient beds)') then 'Community'
      when [Site_Type] in ('1. General acute hospital','2. Specialist hospital (acute only)','GENERAL ACUTE HOSPITAL',
      'Specialist hospital (acute only)') then 'Acute'
      when [Site_Type] in ('4. Mental Health (including Specialist services)','5. Learning Disabilities',
      '6. Mental Health and Learning Disabilities''','Learning Disabilities','Mental Health','Mental Health (including Specialist services)',
      'Mental Health and Learning Disabilities)') then 'MHLDA'
      when [Site_Type] in ('3. Mixed service hospital','Mixed service hospital','MULTI-SERVICE HOSPITAL') then 'Multi'
      else 'Other'
      end as Site_Class
    ,[Tenure]
    ,[Non_Inpatient_Type]
    ,[Type_Of_Lease]
    ,[Measure_Category]
    ,[Measure_Name]
    ,[Measure_Value]
    ,cast([Measure_Value_Str] as nvarchar) as [Measure_Value_Str]
    ,[Effective_Snapshot_Date]
    ,cast(datepart(yyyy,[Effective_Snapshot_Date])-1 as varchar)+'-'+cast(datepart(yyyy,[Effective_Snapshot_Date]) as varchar) as der_fin_year
    ,[DataSourceFileForThisSnapshot_Version]
    ,[Report_Period_Length]
    ,[Unique_ID]
    ,[AuditKey]
FROM [UKHF_Estates_Returns_Information_Collection].[Site_Data_V21_1]
where [Site_Type] is not NULL
),

final AS (
  SELECT *,
    CASE WHEN Org_Class = 'Community' AND Site_Type IN ('SHORT TERM NON-ACUTE HOSPITAL','LONG STAY HOSPITAL','SPECIALIST HOSPITAL') THEN 'Community'
         ELSE Site_Class END AS Site_Class2
  FROM src
),

part AS (
  Select Organisation_Code, Organisation_Type, Org_Class, [Status], Site_Code, Site_Type, Site_Class, Tenure, Non_Inpatient_Type, Type_Of_Lease, Measure_Category,
  Measure_Name, Measure_Value, Measure_Value_Str, Effective_Snapshot_Date,
  CASE WHEN der_fin_year = '2024-2025' then '2025-2026' end as der_fin_year, DataSourceFileForThisSnapshot_Version, Report_Period_Length, Unique_ID, AuditKey,
    CASE WHEN Org_Class = 'Community' AND Site_Type IN ('SHORT TERM NON-ACUTE HOSPITAL','LONG STAY HOSPITAL','SPECIALIST HOSPITAL') THEN 'Community'
         ELSE Site_Class END AS Site_Class2
  FROM src
  where der_fin_year = '2024-2025'
)

SELECT DISTINCT
  Organisation_Code, Organisation_Type, Org_Class, Site_Code, Site_Type, Site_Class2 AS Site_Class, der_fin_year
FROM final
WHERE LEFT(Site_Code,3) != 'AGG'
union all
SELECT DISTINCT
  Organisation_Code, Organisation_Type, Org_Class, Site_Code, Site_Type, Site_Class2 AS Site_Class, der_fin_year
FROM part
WHERE LEFT(Site_Code,3) != 'AGG'
ORDER BY Organisation_Code, Site_Code, der_fin_year
  "
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}