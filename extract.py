from pymongo import MongoClient
from dagster import op, Out, In, DagsterType
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime
import pandas as pd

mongo_connection_string = "mongodb://dap:dap@127.0.0.1"
#Vaccination_Stg
Covid19Vaccination_stg = create_dagster_pandas_dataframe_type(
    name="Covid19Vaccination_stg",
    columns=[
        PandasColumn.string_column("_id", non_nullable=True), 
        PandasColumn.datetime_column("date",min_datetime=datetime(year=2010, month=1, day=1),max_datetime=datetime(year=2022, month=11, day=30), non_nullable=True),        
        PandasColumn.string_column("fips"),
        PandasColumn.string_column("mmwr_week"),
        PandasColumn.string_column("recip_county"),
        PandasColumn.string_column("recip_state"),
        PandasColumn.string_column("completeness_pct"),
        PandasColumn.string_column("administered_dose1_recip")
        PandasColumn.string_column("administered_dose1_pop_pct"),
        #PandasColumn.integer_column("administered_dose1_recip_5plus"),
        #PandasColumn.numeric_column("administered_dose1_recip_5pluspop_pct"),
        #PandasColumn.integer_column("administered_dose1_recip_12plus"),
        #PandasColumn.numeric_column("administered_dose1_recip_12pluspop_pct"),
        #PandasColumn.integer_column("administered_dose1_recip_18plus")
        #PandasColumn.numeric_column("administered_dose1_recip_18pluspop_pct"),
        #PandasColumn.integer_column("administered_dose1_recip_65plus"),
        #PandasColumn.numeric_column("administered_dose1_recip_65pluspop_pct"),
        PandasColumn.string_column("series_complete_yes"),
        PandasColumn.string_column("series_complete_pop_pct"),
        #PandasColumn.integer_column("series_complete_5plus"),
        #PandasColumn.numeric_column("series_complete_5pluspop_pct")
        #PandasColumn.integer_column("series_complete_5to17"),
        #PandasColumn.numeric_column("series_complete_5to17pop_pct"),
        #PandasColumn.integer_column("series_complete_12plus"),
        #PandasColumn.numeric_column("series_complete_12pluspop_pct"),
        #PandasColumn.integer_column("Series_Complete_18Plus"),
        #PandasColumn.numeric_column("Series_Complete_18PlusPop_Pct")
        #PandasColumn.integer_column("Series_Complete_65Plus"),
        #PandasColumn.numeric_column("Series_Complete_65PlusPop_Pct"),
        PandasColumn.string_column("Booster_Doses"),
        PandasColumn.string_column("Booster_Doses_Vax_Pct"),
        #PandasColumn.integer_column("Booster_Doses_5Plus"),
        #PandasColumn.numeric_column("Booster_Doses_5Plus_Vax_Pct"),
        #PandasColumn.integer_column("Booster_Doses_12Plus")
        #PandasColumn.numeric_column("Booster_Doses_12Plus_Vax_Pct"),
        #PandasColumn.integer_column("Booster_Doses_18Plus"),
        #PandasColumn.numeric_column("Booster_Doses_18Plus_Vax_Pct"),
        #PandasColumn.integer_column("Booster_Doses_50Plus"),
        #PandasColumn.numeric_column("Booster_Doses_50Plus_Vax_Pct"),
        #PandasColumn.integer_column("Booster_Doses_65Plus")
        #PandasColumn.numeric_column("Booster_Doses_65Plus_Vax_Pct"),
        #PandasColumn.integer_column("Second_Booster_50Plus"),
        #PandasColumn.numeric_column("Second_Booster_50Plus_Vax_Pct"),
        #PandasColumn.integer_column("Second_Booster_65Plus"),
        #PandasColumn.numeric_column("Second_Booster_65Plus_Vax_Pct"),
        PandasColumn.string_column("SVI_CTGY"),
        #PandasColumn.numeric_column("Series_Complete_Pop_Pct_SVI")
        #PandasColumn.numeric_column("Series_Complete_5PlusPop_Pct_SVI"),
        #PandasColumn.numeric_column("Series_Complete_5to17Pop_Pct_SVI"),
        #PandasColumn.numeric_column("Series_Complete_12PlusPop_Pct_SVI"),
        #PandasColumn.numeric_column("Series_Complete_18PlusPop_Pct_SVI"),
        #PandasColumn.numeric_column("Series_Complete_65PlusPop_Pct_SVI"),
        PandasColumn.string_column("Metro_status")
        PandasColumn.string_column("Series_Complete_Pop_Pct_UR_Equity"),
        #PandasColumn.numeric_column("Series_Complete_5PlusPop_Pct_UR_Equity"),
        #PandasColumn.numeric_column("Series_Complete_5to17Pop_Pct_UR_Equity"),
        #PandasColumn.numeric_column("Series_Complete_12PlusPop_Pct_UR_Equity"),
        #PandasColumn.numeric_column("Series_Complete_18PlusPop_Pct_UR_Equity"),
        #PandasColumn.numeric_column("Series_Complete_65PlusPop_Pct_UR_Equity"),
        PandasColumn.string_column("Booster_Doses_Vax_Pct_SVI")
        #PandasColumn.numeric_column("Booster_Doses_12PlusVax_Pct_SVI"),
        #PandasColumn.numeric_column("Booster_Doses_18PlusVax_Pct_SVI"),
        #PandasColumn.numeric_column("Booster_Doses_65PlusVax_Pct_SVI"),
        PandasColumn.numeric_column("Booster_Doses_Vax_Pct_UR_Equity"),
        #PandasColumn.numeric_column("Booster_Doses_12PlusVax_Pct_UR_Equity"),
        #PandasColumn.numeric_column("Booster_Doses_18PlusVax_Pct_UR_Equity")
        #PandasColumn.numeric_column("Booster_Doses_65PlusVax_Pct_UR_Equity"),
        PandasColumn.string_column("Census2019"),
        #PandasColumn.integer_column("Census2019_5PlusPop"),
        #PandasColumn.integer_column("Census2019_5to17Pop"),
        #PandasColumn.integer_column("Census2019_12PlusPop"),
        #PandasColumn.integer_column("Census2019_18PlusPop")
        #PandasColumn.integer_column("Census2019_65PlusPop"),
        PandasColumn.string_column("Bivalent_Booster_5Plus")
        #PandasColumn.numeric_column("Bivalent_Booster_5Plus_Pop_Pct"),
        #PandasColumn.integer_column("Bivalent_Booster_65Plus"),
        #PandasColumn.integer_column("Bivalent_Booster_12Plus"),
        #PandasColumn.numeric_column("Bivalent_Booster_12Plus_Pop_Pct"),
        #PandasColumn.integer_column("Bivalent_Booster_18Plus"),
        #PandasColumn.numeric_column("Bivalent_Booster_18Plus_Pop_Pct"),
        #PandasColumn.numeric_column("Bivalent_Booster_65Plus_Pop_Pct")
    ],
)
#Demographic_stg
Covid19Vactn_Dmgrph_stg = create_dagster_pandas_dataframe_type(
    name="Covid19Vactn_Dmgrph_stg",
    columns=[ 
        PandasColumn.string_column("_id",non_nullable=True),
        PandasColumn.datetime_column("date",min_datetime=datetime(year=2010, month=1, day=1),max_datetime=datetime(year=2022, month=11, day=30),non_nullable=True),
        PandasColumn.string_column("demographic_category",  non_nullable=True),  
        PandasColumn.integer_column("administered_dose1"),
        PandasColumn.numeric_column("administered_dose1_pct_known",  non_nullable=True),
        PandasColumn.numeric_column("administered_dose1_pct_us",  non_nullable=True),
        PandasColumn.string_column("series_complete_yes",non_nullable=True),
        PandasColumn.numeric_column("administered_dose1_pct",  non_nullable=True),
        PandasColumn.numeric_column("series_complete_pop_pct",  non_nullable=True),
        PandasColumn.numeric_column("series_complete_pop_pct_known",  non_nullable=True),  
        PandasColumn.integer_column("series_complete_pop_pct_us"),
        PandasColumn.numeric_column("booster_doses_vax_pct_agegroup",  non_nullable=True),
        PandasColumn.numeric_column("booster_doses_pop_pct_known",  non_nullable=True),
        PandasColumn.numeric_column("booster_doses_vax_pct_us",non_nullable=True),
        PandasColumn.numeric_column("booster_doses_pop_pct_known_last14days",  non_nullable=True),
        PandasColumn.integer_column("booster_doses_yes",  non_nullable=True),
        PandasColumn.integer_column("booster_doses_yes_last14days",  non_nullable=True),
        PandasColumn.numeric_column("second_booster_vax_pct_agegroup",  non_nullable=True)
        PandasColumn.numeric_column("second_booster_pop_pct_known"),
        PandasColumn.numeric_column("second_booster_pop_pct_us",  non_nullable=True),
        PandasColumn.numeric_column("second_booster_pop_pct_known_last14days",  non_nullable=True),
        PandasColumn.string_column("second_booster",non_nullable=True),
        PandasColumn.string_column("second_booster_last14days",  non_nullable=True),
        PandasColumn.string_column("bivalent_booster",  non_nullable=True),
        PandasColumn.numeric_column("bivalent_booster_pop_pct_agegroup",  non_nullable=True),
        PandasColumn.numeric_column("bivalent_booster_pop_pct_known",  non_nullable=True)
    ],
)
## Death_Stg
Covid19_Death_Stg = create_dagster_pandas_dataframe_type(
    name="Covid19_Death_Stg",
    columns=[
        PandasColumn.string_column("_id"),
        PandasColumn.datetime_column("submission_date",min_datetime=datetime(year=2010, month=1, day=1),max_datetime=datetime(year=2022, month=11, day=30)),
        PandasColumn.string_column("state"),
        PandasColumn.integer_column("tot_cases"),
        PandasColumn.numeric_column("conf_cases"),
        PandasColumn.integer_column("prob_cases"),
        PandasColumn.numeric_column("new_case"),
        PandasColumn.integer_column("pnew_case"),
        PandasColumn.integer_column("tot_death"),
        PandasColumn.numeric_column("conf_death"),
        PandasColumn.integer_column("prob_death"),
        PandasColumn.numeric_column("new_death")
        PandasColumn.integer_column("pnew_death"),
        PandasColumn.datetime_column("created_at")
        PandasColumn.string_column("consent_cases"),
        PandasColumn.string_column("consent_deaths")
    ],
)
## TBD STG_US_VHCLE_COLLSN
Stg_US_Vhcle_Collsn = create_dagster_pandas_dataframe_type(
    name="Stg_US_Vhcle_Collsn",
    columns=[
        PandasColumn.integer_column("_id",non_nullable=True, unique=True),
        PandasColumn.string_column("unique_id",non_nullable=True, unique=True),
        PandasColumn.string_column("collision_id"),
        PandasColumn.integer_column("crash_date"),
        PandasColumn.integer_column("crash_time"),
        PandasColumn.integer_column("vehicle_id"),
        PandasColumn.string_column("state_registration"),
        PandasColumn.string_column("vehicle_type"),
        PandasColumn.string_column("vehicle_make"),
        PandasColumn.integer_column("vehicle_model"),
        PandasColumn.string_column("vehicle_year"),
        PandasColumn.string_column("travel_direction"),
        PandasColumn.integer_column("vehicle_occupants"),
        PandasColumn.integer_column("driver_sex"),
        PandasColumn.integer_column("driver_license_status"),
        PandasColumn.string_column("driver_license_jurisdiction"),
        PandasColumn.string_column("pre_crash"),
        PandasColumn.string_column("point_of_impact"),
        PandasColumn.string_column("vehicle_damage"),
        PandasColumn.integer_column("vehicle_damage_1"),
        PandasColumn.integer_column("vehicle_damage_2"),
        PandasColumn.integer_column("vehicle_damage_3"),
        PandasColumn.string_column("public_property_damage"),
        PandasColumn.string_column("public_property_damage_type"),
        PandasColumn.string_column("contributing_factor_1"),
        PandasColumn.string_column("contributing_factor_2")
    ],
)

def is_tuple(_, value):
    return isinstance(value, tuple) and all(
        isinstance(element, datetime) for element in value
    )

DateTuple = DagsterType(
    name="DateTuple",
    type_check_fn=is_tuple,
    description="A tuple of scalar values",
)

Covid19Vaccination_columns = {
    "_id" : "_id",
"date" : "date",
"fips" : "fips",
"mmwr_week" : "mmwr_week",
"recip_county" :"recip_county",
"recip_state":"recip_state" ,
"completeness_pct" : "completeness_pct",
"administered_dose1_recip" :"administered_dose1_recip",
"administered_dose1_pop_pct" :"administered_dose1_pop_pct" ,
#"administered_dose1_recip_5plus" : "administered_dose1_recip_5plus",
#"administered_dose1_recip_5pluspop_pct" : "administered_dose1_recip_5pluspop_pct",
#"administered_dose1_recip_12plus" : "administered_dose1_recip_12plus",
#"administered_dose1_recip_12pluspop_pct": "administered_dose1_recip_12pluspop_pct",
#"administered_dose1_recip_18plus" : "administered_dose1_recip_18plus",
#"administered_dose1_recip_18pluspop_pct": "administered_dose1_recip_18pluspop_pct",
#"administered_dose1_recip_65plus" : "administered_dose1_recip_65plus",
#"administered_dose1_recip_65pluspop_pct": "administered_dose1_recip_65pluspop_pct",
"series_complete_yes" : "series_complete_yes",
"series_complete_pop_pct":"series_complete_pop_pct",
#"series_complete_5plus":"series_complete_5plus" ,
#"series_complete_5pluspop_pct" : "series_complete_5pluspop_pct" ,
#"series_complete_5to17":"series_complete_5to17" ,
#"series_complete_5to17pop_pct" : "series_complete_5to17pop_pct" ,
#"series_complete_12plus" :"series_complete_12plus",
#"series_complete_12pluspop_pct": "series_complete_12pluspop_pct",
#"Series_Complete_18Plus" :"Series_Complete_18Plus",
#"Series_Complete_18PlusPop_Pct": "Series_Complete_18PlusPop_Pct",
#"Series_Complete_65Plus" :"Series_Complete_65Plus",
#"Series_Complete_65PlusPop_Pct": "Series_Complete_65PlusPop_Pct",
"Booster_Doses": "Booster_Doses" ,
"Booster_Doses_Vax_Pct":"Booster_Doses_Vax_Pct" ,
#"Booster_Doses_5Plus" : "Booster_Doses_5Plus",
#"Booster_Doses_5Plus_Vax_Pct": "Booster_Doses_5Plus_Vax_Pct",
#"Booster_Doses_12Plus":"Booster_Doses_12Plus",
#"Booster_Doses_12Plus_Vax_Pct" : "Booster_Doses_12Plus_Vax_Pct" ,
#"Booster_Doses_18Plus":"Booster_Doses_18Plus",
#"Booster_Doses_18Plus_Vax_Pct" : "Booster_Doses_18Plus_Vax_Pct" ,
#"Booster_Doses_50Plus":"Booster_Doses_50Plus",
#"Booster_Doses_50Plus_Vax_Pct" : "Booster_Doses_50Plus_Vax_Pct" ,
#"Booster_Doses_65Plus":"Booster_Doses_65Plus",
#"Booster_Doses_65Plus_Vax_Pct" : "Booster_Doses_65Plus_Vax_Pct" ,
#"Second_Booster_50Plus":"Second_Booster_50Plus" ,
#"Second_Booster_50Plus_Vax_Pct": "Second_Booster_50Plus_Vax_Pct",
#"Second_Booster_65Plus":"Second_Booster_65Plus" ,
#"Second_Booster_65Plus_Vax_Pct": "Second_Booster_65Plus_Vax_Pct",
"SVI_CTGY": "SVI_CTGY",
#"Series_Complete_Pop_Pct_SVI": "Series_Complete_Pop_Pct_SVI",
#"Series_Complete_5PlusPop_Pct_SVI" : "Series_Complete_5PlusPop_Pct_SVI" ,
#"Series_Complete_5to17Pop_Pct_SVI" : "Series_Complete_5to17Pop_Pct_SVI" ,
#"Series_Complete_12PlusPop_Pct_SVI" : "Series_Complete_12PlusPop_Pct_SVI",
#"Series_Complete_18PlusPop_Pct_SVI" : "Series_Complete_18PlusPop_Pct_SVI",
#"Series_Complete_65PlusPop_Pct_SVI" : "Series_Complete_65PlusPop_Pct_SVI",
"Metro_status" :"Metro_status",
"Series_Complete_Pop_Pct_UR_Equity" : "Series_Complete_Pop_Pct_UR_Equity",
#"Series_Complete_5PlusPop_Pct_UR_Equity": "Series_Complete_5PlusPop_Pct_UR_Equity",
#"Series_Complete_5to17Pop_Pct_UR_Equity": "Series_Complete_5to17Pop_Pct_UR_Equity",
#"Series_Complete_12PlusPop_Pct_UR_Equity" : "Series_Complete_12PlusPop_Pct_UR_Equity" ,
#"Series_Complete_18PlusPop_Pct_UR_Equity" : "Series_Complete_18PlusPop_Pct_UR_Equity" ,
#"Series_Complete_65PlusPop_Pct_UR_Equity" : "Series_Complete_65PlusPop_Pct_UR_Equity" ,
"Booster_Doses_Vax_Pct_SVI":"Booster_Doses_Vax_Pct_SVI",
#"Booster_Doses_12PlusVax_Pct_SVI" : "Booster_Doses_12PlusVax_Pct_SVI",
#"Booster_Doses_18PlusVax_Pct_SVI" : "Booster_Doses_18PlusVax_Pct_SVI",
#"Booster_Doses_65PlusVax_Pct_SVI" : "Booster_Doses_65PlusVax_Pct_SVI",
"Booster_Doses_Vax_Pct_UR_Equity" : "Booster_Doses_Vax_Pct_UR_Equity",
#"Booster_Doses_12PlusVax_Pct_UR_Equity" : "Booster_Doses_12PlusVax_Pct_UR_Equity",
#"Booster_Doses_18PlusVax_Pct_UR_Equity" : "Booster_Doses_18PlusVax_Pct_UR_Equity",
#"Booster_Doses_65PlusVax_Pct_UR_Equity" : "Booster_Doses_65PlusVax_Pct_UR_Equity",
"Census2019" :"Census2019",
#"Census2019_5PlusPop" : "Census2019_5PlusPop",
#"Census2019_5to17Pop" : "Census2019_5to17Pop",
#"Census2019_12PlusPop":"Census2019_12PlusPop",
#"Census2019_18PlusPop":"Census2019_18PlusPop",
#"Census2019_65PlusPop":"Census2019_65PlusPop",
"Bivalent_Booster_5Plus" :"Bivalent_Booster_5Plus"
#"Bivalent_Booster_5Plus_Pop_Pct" : "Bivalent_Booster_5Plus_Pop_Pct",
#"Bivalent_Booster_65Plus":"Bivalent_Booster_65Plus",
#"Bivalent_Booster_12Plus":"Bivalent_Booster_12Plus",
#"Bivalent_Booster_12Plus_Pop_Pct" : "Bivalent_Booster_12Plus_Pop_Pct",
#"Bivalent_Booster_18Plus":"Bivalent_Booster_18Plus",
#"Bivalent_Booster_18Plus_Pop_Pct" : "Bivalent_Booster_18Plus_Pop_Pct",
#"Bivalent_Booster_65Plus_Pop_Pct" : "Bivalent_Booster_65Plus_Pop_Pct"
}

#1st Data set
@op(ins={'start': In(bool)}, out=Out(Covid19Vaccination_stg))
def extract_covid19_vaccinations(start) -> Covid19Vaccination_stg:
    conn = MongoClient(mongo_connection_string)
    db = conn["projectdb"]
    covid19_vaccinations = pd.DataFrame(db.covid19_vaccinations.find({}))
    covid19_vaccinations.drop(
        columns=["_id"],
        axis=1,
        inplace=True
    )
    covid19_vaccinations.rename(
        columns=Covid19Vaccination_columns,
        inplace=True
    )
    conn.close()
    return covid19_vaccinations

@op(ins={'covid19_vaccinations': In(Covid19Vaccination_stg)}, out=Out(None))
def stage_extracted_covid19_vaccinations(covid19_vaccinations):
    covid19_vaccinations.to_csv("staging/Covid19Vaccination.csv",index=False,sep="\t")
    
##2nd Data set
Covid19Vaccination_Dmgrph_Columns = {"_id":"_id"
"dateTIMESTAMP":"dateTIMESTAMP"
"demographic_category":"demographic_category"
"administered_dose1integer":"administered_dose1integer"
"administered_dose1_pct_known":"administered_dose1_pct_known"
"administered_dose1_pct_us":"administered_dose1_pct_us"
"series_complete_yesinteger":"series_complete_yesinteger"
"administered_dose1_pct":"administered_dose1_pct"
"series_complete_pop_pct":"series_complete_pop_pct"
"series_complete_pop_pct_known":"series_complete_pop_pct_known"
"series_complete_pop_pct_us":"series_complete_pop_pct_us"
"booster_doses_vax_pct_agegroup":"booster_doses_vax_pct_agegroup"
"booster_doses_pop_pct_known":"booster_doses_pop_pct_known"
"booster_doses_vax_pct_us":"booster_doses_vax_pct_us"
"booster_doses_pop_pct_known_last14days":"booster_doses_pop_pct_known_last14days"
"booster_doses_yesinteger":"booster_doses_yesinteger"
"booster_doses_yes_last14daysinteger":"booster_doses_yes_last14daysinteger"
"second_booster_vax_pct_agegroup":"second_booster_vax_pct_agegroup"
"second_booster_pop_pct_known":"second_booster_pop_pct_known"
"second_booster_pop_pct_us":"second_booster_pop_pct_us"
"second_booster_pop_pct_known_last14days":"second_booster_pop_pct_known_last14days"
"second_booster":"second_booster"
"second_booster_last14days":"second_booster_last14days"
"bivalent_booster":"bivalent_booster"
"bivalent_booster_pop_pct_agegroup":"bivalent_booster_pop_pct_agegroup"
"bivalent_booster_pop_pct_known":"bivalent_booster_pop_pct_known"}

@op(ins={'start': In(bool)}, out=Out(Covid19Vaccination_Dmgrph_stg))

def extract_covid19_vacc_demografic(start) -> Covid19Vaccination_Dmgrph_stg:
    conn = MongoClient(mongo_connection_string)
    db = conn["projectdb"]
    covid19_vacc_demografic = pd.DataFrame(db.covid19_vacc_demografic.find({}))
    covid19_vacc_demografic.drop(
        columns=["_id"],
        axis=1,
        inplace=True
    )    
    conn.close()
    return covid19_vacc_demografic

@op(ins={'covid19_vacc_demografic': In(CustomersDataFrame)}, out=Out(None))
def stage_extracted_covid19_vacc_demografic(covid19_vacc_demografic):
    covid19_vacc_demografic.to_csv("staging/covid19_vacc_demografic.csv",index=False,sep="\t")
    
##3rd Data set
covid19_us_death_columns ={
"_id":"_id"
"submission_date":"submission_date
"state":"state"
"tot_cases":"tot_cases"
"conf_cases":"conf_cases"
"prob_cases":"prob_cases"
"new_case":"new_case"
"pnew_case":"pnew_case"
"tot_death":"tot_death"
"conf_death":"conf_death"
"prob_death":"prob_death"
"new_death":"new_death"
"pnew_death":"pnew_death"
"created_at":"created_at"
"consent_cases":"consent_cases"
"consent_deaths":"consent_deaths"}
@op(ins={'start': In(bool)}, out=Out(covid19_us_death_stg))
def extract_covid19_us_death(start) -> covid19_us_death_stg:
    conn = MongoClient(mongo_connection_string)
    db = conn["projectdb"]
    covid19_us_death = pd.DataFrame(db.covid19_us_death.find({}))
    covid19_us_death.drop(
        columns=["_id"],
        axis=1,
        inplace=True
    )
    
    conn.close()
    return covid19_us_death

@op(ins={'covid19_us_death': In(covid19_us_death_stg)}, out=Out(None))
def stage_extracted_covid19_us_death(covid19_us_death):
    covid19_us_death.to_csv("staging/covid19_us_death.csv",index=False,sep="\t")
    
    
##4th Data set
motor_vehicle_collisn={
"_id":"_id"     
"crash_date":"crash_date"
"crash_time":"crash_time"
"vehicle_id":"vehicle_id"
"state_registration":"state_registration" 
"vehicle_type":"vehicle_type" 
"vehicle_make":"vehicle_make" 
"vehicle_model":"vehicle_model"
"vehicle_year":"vehicle_year" 
"travel_direction":"travel_direction" 
"vehicle_occupants":"vehicle_occupants"
"driver_sex character":"driver_sex character" 
"driver_license_status":"driver_license_status" 
"driver_license_jurisdiction character":"driver_license_jurisdiction character" 
"pre_crash":"pre_crash"
"point_of_impact":"point_of_impact" 
"vehicle_damage":"vehicle_damage"
"vehicle_damage_1":"vehicle_damage_1" 
"vehicle_damage_2":"vehicle_damage_2" 
"vehicle_damage_3":"vehicle_damage_3" 
"public_property_damage character":"public_property_damage character" 
"public_property_damage_type":"public_property_damage_type" 
"contributing_factor_1":"contributing_factor_1" 
"contributing_factor_2":"contributing_factor_2"
}
@op(ins={'start': In(bool)}, out=Out(motor_vehicle_collisn_stg))
def extract_covid19_us_death(start) -> motor_vehicle_collisn_stg:
    conn = MongoClient(mongo_connection_string)
    db = conn["projectdb"]
    motor_vehicle_collisn = pd.DataFrame(db.motor_vehicle_collisn.find({}))
    motor_vehicle_collisn.drop(
        columns=["_id"],
        axis=1,
        inplace=True
    )
    
    conn.close()
    return covid19_us_death

@op(ins={'motor_vehicle_collisn': In(motor_vehicle_collisn_stg)}, out=Out(None))
def stage_extracted_covid19_us_death(covid19_us_death):
    covid19_us_death.to_csv("staging/covid19_us_death.csv",index=False,sep="\t")
