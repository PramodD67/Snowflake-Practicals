snowsql -a  Account Locator + Regional Zone -u pramod67
Password

For example:

Account Locator = MY27323
Region = ap-southeast-1

Query: snowsql -a MY27323.ap-southeast-1 -u pramod67;

Praltimsnow@25;
select current_account_name();
select current_region(); --AWS_AP_SOUTHEAST_1

 snowsql -a MY27323.AWS-AP-SOUTHEAST-1 -u pramod67;

 SELECT CURRENT_ACCOUNT(), CURRENT_REGION();