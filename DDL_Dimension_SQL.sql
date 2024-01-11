--Dimension 1
select 
Recorded_DT , 
mmwr_week,
recip_county,
state_name,
cast(completeness_pct as NUMERIC(18,2)),
cast(administered_dose1_recip as integer),
cast(series_complete_yes_stg1 as integer),
cast(booster_doses as integer),
cast(booster_doses_vax_pct as NUMERIC(18,2)),
cast(booster_doses_vax_pct_svi as NUMERIC(18,2)),
cast(census2019 as integer),
cast(series_complete_yes_stg2 as integer),
cast(booster_doses as integer),
svi_ctgy,
metro_status,
demographic_category,
cast(administered_dose1 as integer),
cast(administered_dose1_pct as NUMERIC(18,2)),
cast(series_complete_pop_pct as NUMERIC(18,2)),
cast(booster_doses_vax_pct_agegroup as NUMERIC(18,2)),
cast(booster_doses_yes as integer),
cast(second_booster_vax_pct_agegroup as NUMERIC(18,2)),
cast(second_booster as integer),
cast(bivalent_booster as integer),
CURRENT_TIMESTAMP() as etl_start_dt,
NULL as etl_end_dt
from
  (
    select  
      TO_DATE(STG1.date, 'MM/DD/YYYY') as Recorded_DT,
      STG1.mmwr_week,
      STG1.recip_county,
      state.state_name,
      CASE WHEN (STG1.completeness_pct) IS NULL THEN '0' ELSE (STG1.completeness_pct) end as completeness_pct,
      CASE WHEN (STG1.administered_dose1_recip) IS NULL THEN '0' ELSE (STG1.administered_dose1_recip) end as administered_dose1_recip,
      CASE WHEN (STG1.series_complete_yes) IS NULL THEN '0' ELSE (STG1.series_complete_yes) end as series_complete_yes_stg1,
      CASE WHEN (STG1.booster_doses) IS NULL THEN '0' ELSE (STG1.booster_doses) end as booster_doses,
      CASE WHEN (STG1.booster_doses_vax_pct) IS NULL THEN '0' ELSE (STG1.booster_doses_vax_pct) end as booster_doses_vax_pct,
      CASE WHEN (STG1.booster_doses_vax_pct_svi) IS NULL THEN '0' ELSE (STG1.booster_doses_vax_pct_svi) end as booster_doses_vax_pct_svi,
      CASE WHEN (STG1.census2019) IS NULL THEN '0' ELSE (STG1.census2019) end as census2019,
      CASE WHEN (STG1.Bivalent_Booster_5Plus) IS NULL THEN '0' ELSE (STG1.Bivalent_Booster_5Plus) end as Bivalent_Booster_5Plus,
      STG1.svi_ctgy,
      STG1.metro_status,
      STG2.demographic_category,
      CASE WHEN (STG2.administered_dose1) IS NULL THEN '0' ELSE (STG2.administered_dose1) end as administered_dose1,
      CASE WHEN (STG2.series_complete_yes) IS NULL THEN '0' ELSE (STG2.series_complete_yes) end as series_complete_yes_stg2,
      CASE WHEN (STG2.administered_dose1_pct) IS NULL THEN '0' ELSE (STG2.administered_dose1_pct) end as administered_dose1_pct,
      CASE WHEN (STG2.series_complete_pop_pct) IS NULL THEN '0' ELSE (STG2.series_complete_pop_pct) end as series_complete_pop_pct,
      CASE WHEN (STG2.booster_doses_vax_pct_agegroup) IS NULL THEN '0' ELSE (STG2.booster_doses_vax_pct_agegroup) end as booster_doses_vax_pct_agegroup,
      CASE WHEN (STG2.booster_doses_yes) IS NULL THEN '0' ELSE (STG2.booster_doses_yes) end as booster_doses_yes,
      CASE WHEN (STG2.second_booster_vax_pct_agegroup) IS NULL THEN '0' ELSE (STG2.second_booster_vax_pct_agegroup) end as second_booster_vax_pct_agegroup,
      CASE WHEN (STG2.second_booster) IS NULL THEN '0' ELSE (STG2.second_booster) end as second_booster,
      CASE WHEN (STG2.bivalent_booster) IS NULL THEN '0' ELSE (STG2.bivalent_booster) end as bivalent_booster
    from
      (
        select
          date,
          fips,
          mmwr_week,
          recip_county,
          recip_state,
          completeness_pct,
          administered_dose1_recip,
          series_complete_yes,
          booster_doses,
          booster_doses_vax_pct,
          svi_ctgy,
          metro_status,
          booster_doses_vax_pct_svi,
          census2019,
		  Bivalent_Booster_5Plus
        FROM
          public.stg_covid19_vaccn
      ) STG1
      left join (
        SELECT
          date,
          demographic_category,
          administered_dose1,
          administered_dose1_pct_known,
          administered_dose1_pct_us,
          series_complete_yes,
          administered_dose1_pct,
          series_complete_pop_pct,
          series_complete_pop_pct_known,
          series_complete_pop_pct_us,
          booster_doses_vax_pct_agegroup,
          booster_doses_pop_pct_known,
          booster_doses_vax_pct_us,
          booster_doses_pop_pct_known_last14days,
          booster_doses_yes,
          booster_doses_yes_last14days,
          second_booster_vax_pct_agegroup,
          second_booster_pop_pct_known,
          second_booster_pop_pct_us,
          second_booster_pop_pct_known_last14days,
          second_booster,
          second_booster_last14days,
          bivalent_booster,
          bivalent_booster_pop_pct_agegroup,
          bivalent_booster_pop_pct_known
        FROM
          public.stg_covid19_vaccn_dmgrphc
      ) STG2 on STG1.date = STG2.date
      left join dim_us_state state on stg1.recip_state = state.state_cd
    where
      STG1.date >= '06/01/2019'
      and STG1.date <= '12/14/2022'
      and completeness_pct <> '0'
  ) DIM1
order by
  Recorded_DT asc
  limit 1000


-- DIMENSION2

