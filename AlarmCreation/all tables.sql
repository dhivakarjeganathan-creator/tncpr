select count(*) from gnb_ericsson;
select count(*) from du_samsung;
select * from du_samsung order by updated_at desc;

select distinct tablename from metricsandtables where lower(metricname) = lower('DU_UPNY_SS_Performance_Issues_CLP');
select * from ruleexecutionresults order by created_at desc where "Id" = '1310103' udc_config_name = 'gnb_acpf_setupfail' order by created_at desc;
select distinct udc_config_name from ruleexecutionresults;

select * from rule_execution_tracking;
select * from performancerules;

select * from batch_jobs;
select * from batchjob_definitions;
select * from batchjob_metrics;
select * from batch_job_storage;

select * from threshold_alarms order by id desc;
select * from threshold_rules where lower(name) like '%corning%';
select * from threshold_generated_queries
select * from carrier_corning order by created_at desc



select metricname, count(*) from threshold_alarms group by metricname

insert into  alerts.status(Node,Severity,Summary,AlertGroup,AlertKey,Identifier,Type,Region) values 
('SOLKTXvGNB-Y-SA-ACPF-1-1212-CMC-01',4,'VM is down','VM','APtest122s3S45','APtestth3s2isSvnfc345',1,'S3-southlake')
Node: NFName
Severity: Thershold Category
Summary: Threshold Alarm_message
AlertGroup: Thershold Name
AlertKey: Record_id
Identifier: Record_id
Type: 2
Region: Region

select * from nfname_results
select * from alarm_last_execution

select * from ericsson_5g_enrichment
select * from samsung_5g_enrichment
select * from regions


select metricname, count(*) from threshold_alarms group by metricname ;
select * from du_samsung where id = '13794012200'
select * from ruleexecutionresults where "Id" = '13704100027' and udc_config_name = 'DU.Samsung.Performance.MMW.CTX'
select distinct udc_config_name from ruleexecutionresults 