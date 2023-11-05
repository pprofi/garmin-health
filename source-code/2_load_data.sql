list @raw_load/garmin/sleep;

select $1,$2
from @raw_load/garmin/sleep/2017-05-05_2017-08-13_62594880_sleepData.json;


-- create json file format

use schema health_data.raw;

create or replace file format ff_json
 type= json
 strip_outer_array=true;

 -- 1. ingest sleep data
create or replace table garmin_sleep_data_raw
(
json_data variant,
source_file varchar
)
comment ='Raw garmin sleep data';
 
 copy into garmin_sleep_data_raw
 from 
 (
 select t.$1 , METADATA$FILENAME
 from @raw_load/garmin/sleep/
   (file_format=>ff_json,
    pattern=>'.*_sleepData.json') t
  );

--    select count(1) from sleep_data_raw

-- 2. parse json sleep data


with cols
as
(
  SELECT DISTINCT
   f.path,
   replace(f.path,'.','_') as column_name,
   typeof(f.value) as column_type
   
FROM 
  sleep_data_raw,
  LATERAL FLATTEN(json_data , RECURSIVE=>true) f  
  WHERE TYPEOF(f.value) != 'OBJECT'
  )
  select 
      'create or replace view vw_garmin_sleep_data \n'||
      'as \n'|| 
      'select '||listagg('\n json_data:'||path||'::'||column_type||' as '||column_name,',')||
      '\n from garmin_sleep_data_raw'
  
  from cols;


select *
from
(
select json_data:sleepScores.recoveryScore x
from sleep_data_raw,
  LATERAL FLATTEN(json_data , RECURSIVE=>true) f  
)
where x is not null


-- view with the parsed sleep data


create or replace view vw_garmin_sleep_data 
as 
select 
 json_data:calendarDate::date as calendarDate,
 json_data:retro::BOOLEAN as retro, 
 TO_TIMESTAMP_NTZ(json_data:sleepEndTimestampGMT::VARCHAR,'yyyy-mm-ddTHH24:mi:ss.ff') as sleepEndTimestampGMT,
 TO_TIMESTAMP_NTZ(json_data:sleepStartTimestampGMT::VARCHAR,'yyyy-mm-ddTHH24:mi:ss.ff') as sleepStartTimestampGMT,
 json_data:sleepWindowConfirmationType::VARCHAR as sleepWindowConfirmationType,
 json_data:awakeSleepSeconds::INTEGER as awakeSleepSeconds,
 json_data:deepSleepSeconds::INTEGER as deepSleepSeconds,
 json_data:remSleepSeconds::INTEGER as remSleepSeconds,
 json_data:unmeasurableSeconds::INTEGER as unmeasurableSeconds,
 json_data:lightSleepSeconds::INTEGER as lightSleepSeconds,
 json_data:averageRespiration::INTEGER as averageRespiration,
 json_data:awakeCount::INTEGER as awakeCount,
 json_data:highestRespiration::INTEGER as highestRespiration,
 json_data:restlessMomentCount::INTEGER as restlessMomentCount,
 json_data:sleepScores.awakeningsCountScore::INTEGER as sleepScores_awakeningsCountScore,
 json_data:sleepScores.combinedAwakeScore::INTEGER as sleepScores_combinedAwakeScore,
 json_data:sleepScores.durationScore::INTEGER as sleepScores_durationScore,
 json_data:sleepScores.feedback::VARCHAR as sleepScores_feedback,
 json_data:sleepScores.insight::VARCHAR as sleepScores_insight,
 json_data:sleepScores.qualityScore::INTEGER as sleepScores_qualityScore,
 json_data:sleepScores.remScore::INTEGER as sleepScores_remScore,
 json_data:sleepScores.restfulnessScore::INTEGER as sleepScores_restfulnessScore,
 json_data:spo2SleepSummary.averageSPO2::INTEGER as spo2SleepSummary_averageSPO2,
 json_data:spo2SleepSummary.lowestSPO2::INTEGER as spo2SleepSummary_lowestSPO2,
 json_data:lowestRespiration::INTEGER as lowestRespiration,
 json_data:sleepScores.deepScore::INTEGER as sleepScores_deepScore,
 json_data:spo2SleepSummary.deviceId::INTEGER as spo2SleepSummary_deviceId,
 json_data:spo2SleepSummary.sleepMeasurementStartGMT::VARCHAR as spo2SleepSummary_sleepMeasurementStartGMT,
 json_data:avgSleepStress::DECIMAL as avgSleepStress,
 json_data:sleepScores.awakeTimeScore::INTEGER as sleepScores_awakeTimeScore,
 json_data:sleepScores.interruptionsScore::INTEGER as sleepScores_interruptionsScore,
 json_data:sleepScores.lightScore::INTEGER as sleepScores_lightScore,
 json_data:sleepScores.recoveryScore::INTEGER as sleepScores_recoveryScore,
 json_data:spo2SleepSummary.sleepMeasurementEndGMT::VARCHAR as spo2SleepSummary_sleepMeasurementEndGMT,
 json_data:spo2SleepSummary.userProfilePk::INTEGER as spo2SleepSummary_userProfilePk,
 json_data:sleepResultType::VARCHAR as sleepResultType,
 json_data:sleepScores.overallScore::INTEGER as sleepScores_overallScore,
 json_data:spo2SleepSummary.averageHR::INTEGER as spo2SleepSummary_averageHR
 from garmin_sleep_data_raw;



-- menstrual cycles
create or replace table garmin_menstrualcycles_data_raw
(
json_data variant,
source_file varchar
)
comment ='Raw garmin menstrual cycles data';
 
 copy into garmin_menstrualcycles_data_raw
 from 
 (
 select t.$1, METADATA$FILENAME
 from @raw_load/garmin/sleep/
   (file_format=>ff_json,
    pattern=>'.*_MenstrualCycles.json') t
  );


with cols
as
(
  SELECT DISTINCT
   f.path,
   replace(f.path,'.','_') as column_name,
   typeof(f.value) as column_type
   
FROM 
  garmin_menstrualcycles_data_raw,
  LATERAL FLATTEN(json_data , RECURSIVE=>true) f  
  WHERE TYPEOF(f.value) != 'OBJECT'
  )
  select 
      'create or replace view garmin_menstrualcycles_data_raw \n'||
      'as \n'|| 
      'select '||listagg('\n json_data:'||path||'::'||column_type||' as '||column_name,',')||
      '\n from garmin_menstrualcycles_data_raw'
  
  from cols;


create or replace view vw_garmin_menstrualcycles_data_raw 
as 
select 
 json_data:actualCycleLength::INTEGER as actualCycleLength,
 json_data:actualPeriodLength::INTEGER as actualPeriodLength,
 json_data:applicableMenstrualCycleLength::INTEGER as applicableMenstrualCycleLength,
 json_data:applicablePeriodLength::INTEGER as applicablePeriodLength,
 json_data:fertileWindowLength::INTEGER as fertileWindowLength,
 json_data:fertileWindowStart::INTEGER as fertileWindowStart,
 json_data:hasLoggedOvulationDay::BOOLEAN as hasLoggedOvulationDay,
 json_data:predictedCycleLength::INTEGER as predictedCycleLength,
 json_data:predictedPeriodLength::INTEGER as predictedPeriodLength,
 json_data:reportTimestamp::VARCHAR as reportTimestamp,
 json_data:startDate::date as startDate,
 json_data:status::VARCHAR as status,
 json_data:userProfilePk::INTEGER as userProfilePk,
 json_data:initialPredictedCycleLength::INTEGER as initialPredictedCycleLength,
 json_data:initialPredictedPeriodLength::INTEGER as initialPredictedPeriodLength,
 json_data:cycleType::VARCHAR as cycleType,
 json_data:createTimestamp::VARCHAR as createTimestamp,
 json_data:hormonalContraception::VARCHAR as hormonalContraception
 from garmin_menstrualcycles_data_raw;


--  
list @raw_load/garmin/


-- daily snapshot

create or replace table garmin_dailysnapshot_data_raw
(
json_data variant,
source_file varchar
)
comment ='Raw garmin daily snapshot data';
 
 copy into garmin_dailysnapshot_data_raw
 from 
 (
 select t.$1, METADATA$FILENAME
 from @raw_load/garmin/user/
   (file_format=>ff_json,
    pattern=>'.*UDSFile_.*.json') t
  );


with cols
as
(
  SELECT DISTINCT
   f.path,
   replace(replace(replace(f.path,'.','_'),'[',''),']','') as column_name,
   typeof(f.value) as column_type
   
FROM 
  garmin_dailysnapshot_data_raw,
  LATERAL FLATTEN(json_data , RECURSIVE=>true) f  
  WHERE TYPEOF(f.value) != 'OBJECT'
  )
  select 
      'create or replace view vw_garmin_dailysnapshot_data_raw \n'||
      'as \n'|| 
      'select '||listagg('\n json_data:'||path||'::'||column_type||' as '||column_name,',')||
      '\n from garmin_dailysnapshot_data_raw'
  
  from cols;


  create or replace view vw_garmin_dailysnapshot_data_raw 
as 
select 
 json_data:activeKilocalories::INTEGER as activeKilocalories,
 json_data:activeSeconds::INTEGER as activeSeconds,
 json_data:allDayStress.aggregatorList::ARRAY as allDayStress_aggregatorList,
 json_data:allDayStress.aggregatorList[0].activityDuration::INTEGER as allDayStress_aggregatorList0_activityDuration,
 json_data:allDayStress.aggregatorList[0].highDuration::INTEGER as allDayStress_aggregatorList0_highDuration,
 json_data:allDayStress.aggregatorList[0].lowDuration::INTEGER as allDayStress_aggregatorList0_lowDuration,
 json_data:allDayStress.aggregatorList[0].restDuration::INTEGER as allDayStress_aggregatorList0_restDuration,
 json_data:allDayStress.aggregatorList[0].stressDuration::INTEGER as allDayStress_aggregatorList0_stressDuration,
 json_data:allDayStress.aggregatorList[0].stressOffWristCount::INTEGER as allDayStress_aggregatorList0_stressOffWristCount,
 json_data:allDayStress.aggregatorList[0].stressTooActiveCount::INTEGER as allDayStress_aggregatorList0_stressTooActiveCount,
 json_data:allDayStress.aggregatorList[0].totalDuration::INTEGER as allDayStress_aggregatorList0_totalDuration,
 json_data:allDayStress.aggregatorList[0].totalStressIntensity::INTEGER as allDayStress_aggregatorList0_totalStressIntensity,
 json_data:allDayStress.aggregatorList[0].uncategorizedDuration::INTEGER as allDayStress_aggregatorList0_uncategorizedDuration,
 json_data:allDayStress.aggregatorList[1].activityDuration::INTEGER as allDayStress_aggregatorList1_activityDuration,
 json_data:allDayStress.aggregatorList[1].averageStressLevel::INTEGER as allDayStress_aggregatorList1_averageStressLevel,
 json_data:allDayStress.aggregatorList[1].highDuration::INTEGER as allDayStress_aggregatorList1_highDuration,
 json_data:allDayStress.aggregatorList[1].lowDuration::INTEGER as allDayStress_aggregatorList1_lowDuration,
 json_data:allDayStress.aggregatorList[1].stressIntensityCount::INTEGER as allDayStress_aggregatorList1_stressIntensityCount,
 json_data:allDayStress.aggregatorList[2].averageStressLevel::INTEGER as allDayStress_aggregatorList2_averageStressLevel,
 json_data:allDayStress.aggregatorList[2].averageStressLevelIntensity::INTEGER as allDayStress_aggregatorList2_averageStressLevelIntensity,
 json_data:allDayStress.aggregatorList[2].highDuration::INTEGER as allDayStress_aggregatorList2_highDuration,
 json_data:allDayStress.aggregatorList[2].maxStressLevel::INTEGER as allDayStress_aggregatorList2_maxStressLevel,
 json_data:allDayStress.aggregatorList[2].mediumDuration::INTEGER as allDayStress_aggregatorList2_mediumDuration,
 json_data:allDayStress.aggregatorList[2].stressIntensityCount::INTEGER as allDayStress_aggregatorList2_stressIntensityCount,
 TO_TIMESTAMP_NTZ(json_data:allDayStress.calendarDate.date::VARCHAR,'MON dd, yyyy HH12:MI:ss AM') as allDayStress_calendarDate_date,
 json_data:bmrKilocalories::INTEGER as bmrKilocalories,
 json_data:durationInMilliseconds::INTEGER as durationInMilliseconds,
 json_data:floorsDescendedInMeters::DECIMAL as floorsDescendedInMeters,
 json_data:maxAvgHeartRate::INTEGER as maxAvgHeartRate,
 json_data:maxHeartRate::INTEGER as maxHeartRate,
 json_data:remainingKilocalories::INTEGER as remainingKilocalories,
 TO_TIMESTAMP_NTZ(json_data:wellnessEndTimeGmt.date::VARCHAR,'MON dd, yyyy HH12:MI:ss AM') as wellnessEndTimeGmt_date,
 json_data:allDayStress.aggregatorList[0].averageStressLevel::INTEGER as allDayStress_aggregatorList0_averageStressLevel,
 json_data:allDayStress.aggregatorList[1].totalStressCount::INTEGER as allDayStress_aggregatorList1_totalStressCount,
 json_data:allDayStress.aggregatorList[1].uncategorizedDuration::INTEGER as allDayStress_aggregatorList1_uncategorizedDuration,
 json_data:allDayStress.aggregatorList[2].lowDuration::INTEGER as allDayStress_aggregatorList2_lowDuration,
 json_data:allDayStress.aggregatorList[2].stressTooActiveCount::INTEGER as allDayStress_aggregatorList2_stressTooActiveCount,
 json_data:allDayStress.aggregatorList[2].type::VARCHAR as allDayStress_aggregatorList2_type,
 json_data:bodyBattery.bodyBatteryVersion::INTEGER as bodyBattery_bodyBatteryVersion,
 json_data:includesActivityData::BOOLEAN as includesActivityData,
 json_data:includesCalorieConsumedData::BOOLEAN as includesCalorieConsumedData,
 json_data:includesWellnessData::BOOLEAN as includesWellnessData,
 json_data:userFloorsAscendedGoal::INTEGER as userFloorsAscendedGoal,
 TO_TIMESTAMP_NTZ(json_data:wellnessEndTimeLocal.date::VARCHAR,'MON dd, yyyy HH12:MI:ss AM') as wellnessEndTimeLocal_date,
 TO_TIMESTAMP_NTZ(json_data:wellnessStartTimeGmt.date::VARCHAR,'MON dd, yyyy HH12:MI:ss AM') as wellnessStartTimeGmt_date,
 json_data:allDayStress.aggregatorList[1].stressTooActiveCount::INTEGER as allDayStress_aggregatorList1_stressTooActiveCount,
 json_data:allDayStress.aggregatorList[1].totalStressIntensity::INTEGER as allDayStress_aggregatorList1_totalStressIntensity,
 json_data:highlyActiveSeconds::INTEGER as highlyActiveSeconds,
 TO_TIMESTAMP_NTZ(json_data:calendarDate.date::VARCHAR,'MON dd, yyyy HH12:MI:ss AM') as calendarDate_date,
 json_data:dailyStepGoal::INTEGER as dailyStepGoal,
 json_data:bodyBattery.bodyBatteryStatList[0].bodyBatteryStatus::VARCHAR as bodyBattery_bodyBatteryStatList0_bodyBatteryStatus,
 TO_TIMESTAMP_NTZ(json_data:bodyBattery.bodyBatteryStatList[1].statTimestamp.date::VARCHAR,'MON dd, yyyy HH12:MI:ss AM') as bodyBattery_bodyBatteryStatList1_statTimestamp_date,
 TO_TIMESTAMP_NTZ(json_data:bodyBattery.bodyBatteryStatList[2].statTimestamp.date::VARCHAR,'MON dd, yyyy HH12:MI:ss AM') as bodyBattery_bodyBatteryStatList2_statTimestamp_date,
 json_data:bodyBattery.chargedValue::INTEGER as bodyBattery_chargedValue,
 json_data:bodyBattery.bodyBatteryStatList[4].statTimestamp.date::VARCHAR as bodyBattery_bodyBatteryStatList4_statTimestamp_date,
 json_data:allDayStress.aggregatorList[0].mediumDuration::INTEGER as allDayStress_aggregatorList0_mediumDuration,
 json_data:allDayStress.aggregatorList[0].totalStressCount::INTEGER as allDayStress_aggregatorList0_totalStressCount,
 json_data:allDayStress.aggregatorList[0].type::VARCHAR as allDayStress_aggregatorList0_type,
 json_data:allDayStress.aggregatorList[1].averageStressLevelIntensity::INTEGER as allDayStress_aggregatorList1_averageStressLevelIntensity,
 json_data:allDayStress.aggregatorList[1].mediumDuration::INTEGER as allDayStress_aggregatorList1_mediumDuration,
 json_data:allDayStress.aggregatorList[2].totalStressCount::INTEGER as allDayStress_aggregatorList2_totalStressCount,
 json_data:allDayStress.aggregatorList[2].totalStressIntensity::INTEGER as allDayStress_aggregatorList2_totalStressIntensity,
 json_data:allDayStress.userProfilePK::INTEGER as allDayStress_userProfilePK,
 json_data:restingHeartRateTimestamp::VARCHAR as restingHeartRateTimestamp,
 json_data:userIntensityMinutesGoal::INTEGER as userIntensityMinutesGoal,
 json_data:userProfilePK::INTEGER as userProfilePK,
 json_data:allDayStress.aggregatorList[0].maxStressLevel::INTEGER as allDayStress_aggregatorList0_maxStressLevel,
 json_data:allDayStress.aggregatorList[0].stressIntensityCount::INTEGER as allDayStress_aggregatorList0_stressIntensityCount,
 json_data:allDayStress.aggregatorList[1].stressOffWristCount::INTEGER as allDayStress_aggregatorList1_stressOffWristCount,
 json_data:allDayStress.aggregatorList[1].totalDuration::INTEGER as allDayStress_aggregatorList1_totalDuration,
 json_data:allDayStress.aggregatorList[2].restDuration::INTEGER as allDayStress_aggregatorList2_restDuration,
 json_data:allDayStress.aggregatorList[2].stressOffWristCount::INTEGER as allDayStress_aggregatorList2_stressOffWristCount,
 json_data:allDayStress.aggregatorList[2].totalDuration::INTEGER as allDayStress_aggregatorList2_totalDuration,
 json_data:bodyBattery.bodyBatteryStatList::ARRAY as bodyBattery_bodyBatteryStatList,
 TO_TIMESTAMP_NTZ(json_data:bodyBattery.calendarDate.date::VARCHAR,'MON dd, yyyy HH12:MI:ss AM') as bodyBattery_calendarDate_date,
 json_data:currentDayRestingHeartRate::INTEGER as currentDayRestingHeartRate,
 json_data:floorsAscendedInMeters::DECIMAL as floorsAscendedInMeters,
 json_data:minHeartRate::INTEGER as minHeartRate,
 json_data:restingHeartRate::INTEGER as restingHeartRate,
 json_data:source::INTEGER as source,
 json_data:totalKilocalories::INTEGER as totalKilocalories,
 json_data:version::INTEGER as version,
 json_data:vigorousIntensityMinutes::INTEGER as vigorousIntensityMinutes,
 json_data:wellnessDistanceMeters::INTEGER as wellnessDistanceMeters,
 json_data:allDayStress.aggregatorList[0].averageStressLevelIntensity::INTEGER as allDayStress_aggregatorList0_averageStressLevelIntensity,
 json_data:allDayStress.aggregatorList[2].stressDuration::INTEGER as allDayStress_aggregatorList2_stressDuration,
 json_data:bodyBattery.userProfilePK::INTEGER as bodyBattery_userProfilePK,
 json_data:minAvgHeartRate::INTEGER as minAvgHeartRate,
 json_data:totalDistanceMeters::INTEGER as totalDistanceMeters,
 json_data:uuid::VARCHAR as uuid,
 json_data:wellnessKilocalories::INTEGER as wellnessKilocalories,
 json_data:netCalorieGoal::INTEGER as netCalorieGoal,
 json_data:bodyBattery.bodyBatteryStatList[0].bodyBatteryStatType::VARCHAR as bodyBattery_bodyBatteryStatList0_bodyBatteryStatType,
 json_data:bodyBattery.bodyBatteryStatList[0].statsValue::INTEGER as bodyBattery_bodyBatteryStatList0_statsValue,
 json_data:bodyBattery.bodyBatteryStatList[1].bodyBatteryStatType::VARCHAR as bodyBattery_bodyBatteryStatList1_bodyBatteryStatType,
 json_data:bodyBattery.bodyBatteryStatList[1].statsValue::INTEGER as bodyBattery_bodyBatteryStatList1_statsValue,
 json_data:bodyBattery.bodyBatteryStatList[2].bodyBatteryStatType::VARCHAR as bodyBattery_bodyBatteryStatList2_bodyBatteryStatType,
 json_data:bodyBattery.bodyBatteryStatList[2].bodyBatteryStatus::VARCHAR as bodyBattery_bodyBatteryStatList2_bodyBatteryStatus,
 json_data:bodyBattery.bodyBatteryStatList[2].statsValue::INTEGER as bodyBattery_bodyBatteryStatList2_statsValue,
 json_data:bodyBattery.bodyBatteryStatList[3].bodyBatteryStatus::VARCHAR as bodyBattery_bodyBatteryStatList3_bodyBatteryStatus,
 json_data:bodyBattery.bodyBatteryStatList[3].statsValue::INTEGER as bodyBattery_bodyBatteryStatList3_statsValue,
 json_data:bodyBattery.drainedValue::INTEGER as bodyBattery_drainedValue,
 json_data:respiration.highestRespirationValue::INTEGER as respiration_highestRespirationValue,
 TO_TIMESTAMP_NTZ(json_data:respiration.latestRespirationTimeGMT.date::VARCHAR,'MON dd, yyyy HH12:MI:ss AM') as respiration_latestRespirationTimeGMT_date,
 json_data:respiration.latestRespirationValue::INTEGER as respiration_latestRespirationValue,
 TO_TIMESTAMP_NTZ(json_data:latestSpo2ValueReadingTimeGmt.date::VARCHAR,'MON dd, yyyy HH12:MI:ss AM') as latestSpo2ValueReadingTimeGmt_date,
 json_data:restingCaloriesFromActivity::INTEGER as restingCaloriesFromActivity,
 json_data:bodyBattery.bodyBatteryStatList[3].bodyBatteryStatType::VARCHAR as bodyBattery_bodyBatteryStatList3_bodyBatteryStatType,
 TO_TIMESTAMP_NTZ(json_data:respiration.calendarDate.date::VARCHAR,'MON dd, yyyy HH12:MI:ss AM') as respiration_calendarDate_date,
 json_data:latestSpo2ValueReadingTimeLocal.date::VARCHAR as latestSpo2ValueReadingTimeLocal_date,
 json_data:totalPushes::INTEGER as totalPushes,
 json_data:allDayStress.aggregatorList[1].restDuration::INTEGER as allDayStress_aggregatorList1_restDuration,
 json_data:allDayStress.aggregatorList[2].uncategorizedDuration::INTEGER as allDayStress_aggregatorList2_uncategorizedDuration,
 TO_TIMESTAMP_NTZ(json_data:wellnessStartTimeLocal.date::VARCHAR,'MON dd, yyyy HH12:MI:ss AM') as wellnessStartTimeLocal_date,
 json_data:allDayStress.aggregatorList[1].maxStressLevel::INTEGER as allDayStress_aggregatorList1_maxStressLevel,
 json_data:allDayStress.aggregatorList[1].stressDuration::INTEGER as allDayStress_aggregatorList1_stressDuration,
 json_data:allDayStress.aggregatorList[1].type::VARCHAR as allDayStress_aggregatorList1_type,
 json_data:allDayStress.aggregatorList[2].activityDuration::INTEGER as allDayStress_aggregatorList2_activityDuration,
 json_data:moderateIntensityMinutes::INTEGER as moderateIntensityMinutes,
 json_data:totalSteps::INTEGER as totalSteps,
 json_data:wellnessTotalKilocalories::INTEGER as wellnessTotalKilocalories,
 json_data:consumedKilocalories::INTEGER as consumedKilocalories,
 json_data:includesAllDayPulseOx::BOOLEAN as includesAllDayPulseOx,
 json_data:includesSingleMeasurement::BOOLEAN as includesSingleMeasurement,
 json_data:includesSleepPulseOx::BOOLEAN as includesSleepPulseOx,
 TO_TIMESTAMP_NTZ(json_data:bodyBattery.bodyBatteryStatList[0].statTimestamp.date::VARCHAR,'MON dd, yyyy HH12:MI:ss AM') as bodyBattery_bodyBatteryStatList0_statTimestamp_date,
 json_data:bodyBattery.bodyBatteryStatList[1].bodyBatteryStatus::VARCHAR as bodyBattery_bodyBatteryStatList1_bodyBatteryStatus,
 TO_TIMESTAMP_NTZ(json_data:bodyBattery.bodyBatteryStatList[3].statTimestamp.date::VARCHAR,'MON dd, yyyy HH12:MI:ss AM') as bodyBattery_bodyBatteryStatList3_statTimestamp_date,
 json_data:respiration.avgWakingRespirationValue::INTEGER as respiration_avgWakingRespirationValue,
 json_data:respiration.lowestRespirationValue::INTEGER as respiration_lowestRespirationValue,
 json_data:respiration.userProfilePK::INTEGER as respiration_userProfilePK,
 json_data:averageSpo2Value::INTEGER as averageSpo2Value,
 json_data:latestSpo2Value::INTEGER as latestSpo2Value,
 json_data:lowestSpo2Value::INTEGER as lowestSpo2Value,
 json_data:pushDistance::INTEGER as pushDistance,
 json_data:bodyBattery.bodyBatteryStatList[4].bodyBatteryStatType::VARCHAR as bodyBattery_bodyBatteryStatList4_bodyBatteryStatType,
 json_data:bodyBattery.bodyBatteryStatList[4].bodyBatteryStatus::VARCHAR as bodyBattery_bodyBatteryStatList4_bodyBatteryStatus,
 json_data:bodyBattery.bodyBatteryStatList[4].statsValue::INTEGER as bodyBattery_bodyBatteryStatList4_statsValue,
 json_data:wellnessActiveKilocalories::INTEGER as wellnessActiveKilocalories,
 json_data:includesContinuousMeasurement::BOOLEAN as includesContinuousMeasurement,
 json_data:burnedKilocalories::INTEGER as burnedKilocalories
 from garmin_dailysnapshot_data_raw;



--select TO_TIMESTAMP_NTZ( 'Apr 15, 2018 12:00:00 AM','MON dd, yyyy HH12:MI:ss AM')
 
select listagg('\ncount('||column_name||') as '|| column_name,',') as cnts,
       listagg(column_name,',') as col_list
from information_schema.columns
where lower(table_name)= 'vw_garmin_dailysnapshot_data_raw'
and column_name like '%DATE%'




SELECT * FROM monthly_sales
    UNPIVOT(cnt FOR cnt IN (jan, feb, mar, april))
    ORDER BY empid;


with
date_col_cnt as
(
select 
count(LATESTSPO2VALUEREADINGTIMELOCAL_DATE) as LATESTSPO2VALUEREADINGTIMELOCAL_DATE,
count(LATESTSPO2VALUEREADINGTIMEGMT_DATE) as LATESTSPO2VALUEREADINGTIMEGMT_DATE,
count(CALENDARDATE_DATE) as CALENDARDATE_DATE,
count(WELLNESSSTARTTIMELOCAL_DATE) as WELLNESSSTARTTIMELOCAL_DATE,
count(ALLDAYSTRESS_CALENDARDATE_DATE) as ALLDAYSTRESS_CALENDARDATE_DATE,
count(WELLNESSSTARTTIMEGMT_DATE) as WELLNESSSTARTTIMEGMT_DATE,
count(WELLNESSENDTIMEGMT_DATE) as WELLNESSENDTIMEGMT_DATE,
count(BODYBATTERY_BODYBATTERYSTATLIST1_STATTIMESTAMP_DATE) as BODYBATTERY_BODYBATTERYSTATLIST1_STATTIMESTAMP_DATE,
count(BODYBATTERY_CALENDARDATE_DATE) as BODYBATTERY_CALENDARDATE_DATE,
count(BODYBATTERY_BODYBATTERYSTATLIST3_STATTIMESTAMP_DATE) as BODYBATTERY_BODYBATTERYSTATLIST3_STATTIMESTAMP_DATE,
count(WELLNESSENDTIMELOCAL_DATE) as WELLNESSENDTIMELOCAL_DATE,
count(RESPIRATION_LATESTRESPIRATIONTIMEGMT_DATE) as RESPIRATION_LATESTRESPIRATIONTIMEGMT_DATE,
count(BODYBATTERY_BODYBATTERYSTATLIST4_STATTIMESTAMP_DATE) as BODYBATTERY_BODYBATTERYSTATLIST4_STATTIMESTAMP_DATE,
count(RESPIRATION_CALENDARDATE_DATE) as RESPIRATION_CALENDARDATE_DATE,
count(BODYBATTERY_BODYBATTERYSTATLIST0_STATTIMESTAMP_DATE) as BODYBATTERY_BODYBATTERYSTATLIST0_STATTIMESTAMP_DATE,
count(BODYBATTERY_BODYBATTERYSTATLIST2_STATTIMESTAMP_DATE) as BODYBATTERY_BODYBATTERYSTATLIST2_STATTIMESTAMP_DATE
from vw_garmin_dailysnapshot_data_raw

)
select * from date_col_cnt
 UNPIVOT (cnt FOR cnts IN (BODYBATTERY_BODYBATTERYSTATLIST1_STATTIMESTAMP_DATE,WELLNESSENDTIMEGMT_DATE,WELLNESSSTARTTIMEGMT_DATE,WELLNESSSTARTTIMELOCAL_DATE,ALLDAYSTRESS_CALENDARDATE_DATE,CALENDARDATE_DATE,BODYBATTERY_BODYBATTERYSTATLIST4_STATTIMESTAMP_DATE,LATESTSPO2VALUEREADINGTIMEGMT_DATE,LATESTSPO2VALUEREADINGTIMELOCAL_DATE,BODYBATTERY_CALENDARDATE_DATE,BODYBATTERY_BODYBATTERYSTATLIST3_STATTIMESTAMP_DATE,WELLNESSENDTIMELOCAL_DATE,RESPIRATION_LATESTRESPIRATIONTIMEGMT_DATE,BODYBATTERY_BODYBATTERYSTATLIST0_STATTIMESTAMP_DATE,BODYBATTERY_BODYBATTERYSTATLIST2_STATTIMESTAMP_DATE,RESPIRATION_CALENDARDATE_DATE))






    /*  
-- summarized activities
select distinct f.path,
   typeof(f.value)
 from @raw_load/garmin/wellness/anastsialife@gmail.com_0_summarizedActivities.json
   (file_format=>ff_json
    ) t,
   lateral flatten(t.$1, recursive=>true) f
    
*/