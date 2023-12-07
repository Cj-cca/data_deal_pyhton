import time
import requests
import base64
import os
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import queue

Table = "Account"
Fields = ["Id", "OwnerId", "IsDeleted", "Name", "CurrencyIsoCode", "CreatedDate", "CreatedById", "LastModifiedDate",
          "LastModifiedById", "SystemModstamp", "LastActivityDate", "LastViewedDate", "LastReferencedDate",
          "Advisory_FY_Sales_Goal__c", "Assurance_FY_Sales_Goal__c", "Client__c", "Tax_FY_Sales_Goal__c",
          "Account_Pipeline_Goal__c", "Asset_Wealth_Management_Trust_US_Rollup__c",
          "Banking_Capital_Markets_Trust_US_Rollup__c", "Cloud_Digital_Consulting_US_Rollup__c",
          "Consumer_Industrial_Products_Trust_US__c", "Cyber_Risk_Reg_Consulting_US_Rollup__c",
          "DAT_ESG_Trust_US_Rollup__c", "Deals_Consulting_US_Rollup__c", "Energy_Utilities_Mining_Trust_US_Rollup__c",
          "Estimated_Sales_Goal__c", "Global_Revenue_Goal__c", "Health_Industries_Trust_US_Rollup__c",
          "Insurance_Trust_US_Rollup__c", "LoS_Audit_Assurance_Rollup__c", "LoS_CMAAS_Assurance_Rollup__c",
          "LoS_Consulting_Advisory_Rollup__c", "LoS_Consulting_US_Rollup__c", "LoS_Cyber_Forensics_Advisory_Rollup__c",
          "LoS_Deals_Advisory_Rollup__c", "LoS_Legal_Tax_Rollup__c", "LoS_P_O_Tax_Rollup__c",
          "LoS_Risk_Assurance_Rollup__c", "LoS_Strategy_Advisory_Rollup__c", "LoS_Tax_Tax_Rollup__c",
          "LoS_Trust_US_Rollup__c", "Managed_Services_Consulting_US_Rollup__c", "Mexico_Trust_US_Rollup__c",
          "PwC_Private_Trust_US_Rollup__c", "Sales_Goal_Variance__c", "Specialties_Trust_US_Rollup__c",
          "Tax_Consulting_Consulting_US_Rollup__c", "Technology_Media_Telecom_Trust_US_Rollup__c",
          "Transformation_Consulting_US_Rollup__c"]


# ["Id", "IsDeleted", "MasterRecordId", "Name", "Type", "RecordTypeId", "ParentId", "BillingStreet",
#       "BillingCity", "BillingState", "BillingPostalCode", "BillingCountry", "BillingLatitude", "BillingLongitude",
#       "BillingGeocodeAccuracy", "ShippingStreet", "ShippingCity", "ShippingState", "ShippingPostalCode",
#       "ShippingCountry", "ShippingLatitude", "ShippingLongitude", "ShippingGeocodeAccuracy", "Phone", "Fax",
#       "AccountNumber", "Website", "PhotoUrl", "Sic", "Industry", "AnnualRevenue", "NumberOfEmployees", "Ownership",
#       "TickerSymbol", "Description", "Rating", "Site", "CurrencyIsoCode", "NameLocal", "OwnerId", "CreatedDate",
#       "CreatedById", "LastModifiedDate", "LastModifiedById", "SystemModstamp", "LastActivityDate", "LastViewedDate",
#       "LastReferencedDate", "IsCustomerPortal", "Jigsaw", "JigsawCompanyId", "AccountSource", "SicDesc",
#       "Alias_Name__c", "Associated_PwC_Office__c", "Client_Group_ID__c", "Client_Source_URL_Hidden__c", "Co_GRP__c",
#       "Competitor__c", "Confidential__c", "Contact_Count__c", "DUNS__c", "Entity_Acceptability_Date__c",
#       "Entity_Acceptability_Status_del__c", "Entity_Acceptability_Territory_del__c", "Ext_Id__c",
#       "Financial_Year_End_del__c", "GUP__c", "Independence_Restriction_Status_del__c", "Industry_Cluster_del__c",
#       "Industry_Sector_del__c", "Industry_classification_del__c", "Internal_PwC_Restriction_Status_del__c",
#       "Internal_PwC_Restriction_Territory_del__c", "JBR__c", "Legal_and_Regulatory_Status_del__c",
#       "Associated_PwC_Office_Deprecated__c", "Org_Count__c", "Organization_Type_del__c", "PO_Required__c",
#       "PRID__c", "Party_Id__c", "Party_Type__c", "PwC_Channel_del__c", "PwC_Office__c", "PwC_Segment_del__c",
#       "TPA_Flag__c", "Ultimate_Parent_Client_Name__c", "Ultimate_Parent__c", "UpdatePartyUsageFlag__c", "Vendor__c",
#       "flagGUP__c", "grp_user__c", "isGPA__c", "isGUP__c", "Open_Opportunties__c", "Opportunity_Total__c",
#       "Client_Count__c", "Account_Count__c", "Account_Version__c", "BXOrganisationID__c", "BoardEx_URL__c",
#       "Client_Source_URL__c", "Internal_Source_Record__c", "Billed_Estimated_Non_Audit_Fees__c", "Contact__c",
#       "Created_from_Contact__c", "CurrentAP_ActiveFrom__c", "CurrentAP_ActiveTo__c", "Current_Account_Plan__c",
#       "Draft_Account_Plan_Deprecated__c", "Fee_Cap_End_Date__c", "Fee_Cap_Start_Date__c", "Fee_Cap_Status__c",
#       "Lotus_Notes_email_only__c", "Non_Audit_Fee_Cap__c", "Percentage_Fee_Cap_Used__c", "Primary_Contact_Email__c",
#       "Primary_Contact_First_Name__c", "Primary_Contact_Last_Name__c", "Primary_Contact_Middle_Name__c",
#       "Tech_Email_Sent__c", "Territory_Email__c", "isLeadConvertedClient__c", "Opportunity_Count__c",
#       "IntroHive__Account_RS_Val__c", "IntroHive__Account_RS__c", "IntroHive__Introhive_Connections_Val__c",
#       "IntroHive__Introhive_Last_Interaction__c", "IntroHive__Introhive_Relationship_Capital_Change__c",
#       "IntroHive__Last_Update_Time__c", "IntroHive__Synced_By__c", "IntroHive__Top_Connected_Colleague__c",
#       "IntroHive__Top_Relationship_Strength__c", "Client_Acceptability_Month__c",
#       "Client_Acceptability_Territory__c", "Client_Acceptability_Year__c", "Entity_Acceptability_Status__c",
#       "Financial_Year_End__c", "Independence_Restriction_Status__c", "Industry_Cluster__c", "Industry_Sector__c",
#       "Industry_classification__c", "Internal_PwC_Restriction_Status__c", "Internal_PwC_Restriction_Territory__c",
#       "Legal_and_Regulatory_Status__c", "Organization_Type__c", "PartyType_Global__c", "PwC_Channel__c",
#       "PwC_Segment__c", "Security_Mode__c", "Original_Owner__c", "Associated_CA_Office__c",
#       "Associated_UK_Office__c", "Associated_US_Office__c", "CA_Prioritisation__c", "UK_Prioritisation__c",
#       "SBQQ__AssetQuantitiesCombined__c", "ByPassFlag__c", "Tech_Country__c", "Tech_State__c",
#       "Ultimate_Parent_Associated_PwC_Office__c", "Ultimate_Parent_Industry_Cluster_UID__c",
#       "Ultimate_Parent_Industry_Cluster__c", "Ultimate_Parent_Industry_Sector_UID__c",
#       "Ultimate_Parent_Industry_Sector__c", "Ultimate_Parent_Prioritisation__c", "Ultimate_Parent_DUNS__c",
#       "Ultimate_Parent_Industry_Classification__c", "Ultimate_Parent_Operating_Country__c",
#       "Ultimate_Parent_PRID__c", "Independence_Restriction_Type__c", "Client_ID__c", "Channel_Preference__c",
#       "Expected_Audit_Tender_Date__c", "Is_Created_From_Contact__c", "US_Prioritisation__c",
#       "Opportunity_Pipeline_Amount__c", "Opportunity_Pipeline_Amount_Rollup__c", "Client_Owner__c",
#       "SkipValidation__c", "SBQQ__CoTermedContractsCombined__c", "Territory__c", "AFS__c", "Deprecated_Nat_ID__c",
#       "Strategists_on_Opportunities__c", "SBQQ__CoTerminationEvent__c", "Primary_Country__c",
#       "Strategists_Edit_Opportu_Deprecated__c", "Client_Verification__c", "API_PartyId__c", "Entity__c",
#       "Security_Context__c", "Security_Country__c", "Security_Legal_Entity__c", "Survivor_Account__c",
#       "Source_Party_ID__c", "SBQQ__ContractCoTermination__c", "SBQQ__DefaultOpportunity__c",
#       "SBQQ__IgnoreParentContractedPrices__c", "SBQQ__PreserveBundle__c", "SBQQ__PriceHoldEnd__c",
#       "SBQQ__RenewalModel__c", "SBQQ__RenewalPricingMethod__c", "SBQQ__TaxExempt__c", "Independence_Entity_Type__c",
#       "US_Top_Account__c", "Update_Ultimate_Parent_Bypass__c", "Operational_Status_Change_Date__c",
#       "Operational_Status__c", "Party_Lifecycle_Date__c", "Party_Lifecycle_Status__c", "To_Be_Deleted__c",
#       "Auditor__c", "Last_Synced_Date__c", "Security_Information__c", "Regulated_Security_Territory__c",
#       "US_Focus_500_for_MC__c", "Financial_Year_End_Date__c", "Expected_Rotation_Date__c",
#       "Final_Channel_Outcome__c"]


class MyThreadPoolExecutor(ThreadPoolExecutor):
    """
    重写线程池修改队列数
    """

    def __init__(self, max_workers=None, thread_name_prefix=''):
        super().__init__(max_workers, thread_name_prefix)
        # 队列大小为最大线程数的两倍
        self._work_queue = queue.Queue(self._max_workers * 10)


class SalesForce(object):
    def __init__(self, env):
        if env == "prod":
            # APIM
            # self.get_url = 'https://api.pwc.com/services/data'
            self.post_url = "https://api.pwcinternal.com:7443/services/data/*"

            # LDAP
            # self.ldap_url = 'LDAP://iam-userstore-prod-iam.pwcinternal.com:636'
            self.ldap_dn = (
                "CN=CN_ifs_Salesforce_p001,OU=Applications,dc=pwcglobal,dc=com"
            )
            self.ldap_pwd = "p8Ks#cop]l)/x9z*?$l^"

            # Credentials
            # self.application_name = 'CN-CMD-Salesforce-dataplatform'
            self.consumer_key = ""
            self.consumer_secret = ""
            self.user_name = ""
            self.password = ""
            self.token = ""
            self.api_key = ""
            self.api_secret = ""
            self.SFDC_Domain = "pwc"
            self.content_tpe = "application/xml"
        else:
            # APIM urls
            # REST API
            # self.get_url = 'https://api-staging.pwc.com/services/data'
            self.post_url = "https://api-staging.pwcinternal.com:7443/services/data/"

            # LDAP
            # self.ldap_url = 'LDAP://iam-userstore-stg-iam.pwcinternal.com:636'
            self.ldap_dn = (
                "CN=CN_ifs_Salesforce_s001,OU=Applications,dc=pwcglobal,dc=com"
            )
            self.ldap_pwd = "}hU/H&Bn/SV*U}Y$EW4^"

            # Credentials
            # self.application_name = 'CN-CMD-Salesforce-dataplatform'
            self.consumer_key = "3MVG9LlLrkcRhGHYwJIWR8uTuTdEk2LCLtignhkl40CY5HsoxbmutF86yfyczZnXHT1ug.F408Y.GRBjA01QE"
            self.consumer_secret = (
                "BA340402881F03549186CFD467CF5D6012A4C1AF3867E8378902F801BA424F5C"
            )
            self.user_name = "warehousedata.chk@pos.eu.prodcopy"
            self.password = "8qw4cTeDXygv3hh539rL"
            self.token = "uA0HBMtYHbIAofYhcfRdQp5k"
            self.api_key = "l7287349fed03145c0b79220d74ff76f6b"
            self.api_secret = "079de51531994fe4bf444e3b3bf3bf09"
            # SFDC_Domain = {'SF_Preview': ['pwc--preview231.sandbox', 'pwc--preview232.sandbox'],
            # 'SF_ProdCopy': 'pwc--prodcopy.sandbox', 'SF_Production': 'pwc'}
            self.SFDC_Domain = "pwc--prodcopy.sandbox"

            self.now = datetime.utcnow()

            self.header_rest = {
                "client_id": self.consumer_key,
                "client_secret": self.consumer_secret,
                "username": self.user_name,
                "password": self.password + self.token,
                "Proxy-Authorization": "Basic "
                                       + str(
                    base64.b64encode(
                        f"{self.ldap_dn.split(',')[0].split('=')[-1]}:{self.ldap_pwd}".encode(
                            "utf-8"
                        )
                    ),
                    encoding="utf-8",
                ),
                "apikey": self.api_key,
                "apikeysecret": self.api_secret,
                "SFDC-Domain": self.SFDC_Domain,
                "Content-Type": "application/xml",
            }

            self.header_bulk = {
                "client_id": self.consumer_key,
                "client_secret": self.consumer_secret,
                "username": self.user_name,
                "password": self.password + self.token,
                "Proxy-Authorization": "Basic "
                                       + str(
                    base64.b64encode(
                        f"{self.ldap_dn.split(',')[0].split('=')[-1]}:{self.ldap_pwd}".encode(
                            "utf-8"
                        )
                    ),
                    encoding="utf-8",
                ),
                "apikey": self.api_key,
                "apikeysecret": self.api_secret,
                "SFDC-Domain": self.SFDC_Domain,
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": "Bearer "
                                 + str(base64.b64encode(self.token.encode("utf-8")), encoding="utf-8"),
            }

            self.proxies = {
                "http": None,
                "https": None,
            }

    def query(self, q):
        url = self.post_url + "v57.0/query"
        params = {"q": q}
        time.sleep(1)
        r = requests.get(
            url=url, params=params, headers=self.header_rest, proxies=self.proxies
        )
        return r.json()

    def create_job(self, q, chunk_size, parent):
        if chunk_size:
            pk_chunking = f"chunkSize={chunk_size};"
        if parent:
            pk_chunking += f" parent={parent}"
        self.header_bulk["Sforce-Enable-PKChunking"] = pk_chunking

        url = self.post_url + "v57.0/jobs/query"
        body = {
            "operation": "query",
            "query": q,
            "contentType": "CSV",
            "columnDelimiter": "PIPE",
            "lineEnding": "CRLF",
        }
        r = requests.post(
            url=url, headers=self.header_bulk, proxies=self.proxies, json=body
        )

        return r.json()

    def query_job(self, job_id):
        url = self.post_url + f"v57.0/jobs/query/{job_id}"
        r = requests.get(url=url, headers=self.header_bulk, proxies=self.proxies)

        return r.json()

    def download_job(self, job_id):
        url = self.post_url + f"v57.0/jobs/query/{job_id}/results"
        r = requests.get(url=url, headers=self.header_bulk, proxies=self.proxies)

        return r.content

    def abort_job(self, job_id):
        url = self.post_url + f"v57.0/jobs/query/{job_id}"
        body = {"state": "Aborted"}
        r = requests.patch(
            url=url, headers=self.header_bulk, proxies=self.proxies, json=body
        )

        return r.json()

    def delete_job(self, job_id):
        url = self.post_url + f"v57.0/jobs/query/{job_id}"
        r = requests.delete(url=url, headers=self.header_bulk, proxies=self.proxies)
        code = r.status_code

        return True if code == 204 else False

    # def query_all_job(self):
    # url = self.post_url + 'v57.0/jobs/query'
    # p = {
    # 'isPkChunkingEnabled': 'true',
    # 'jobType': 'V2Query',
    # 'concurrencyMode': 'parallel',
    # 'queryLocator': 'nextRecordsUrl'
    # }
    # r = requests.get(url=url, headers=self.header_bulk, proxies=self.proxies)
    #
    # return r.text

    def conn_doris(host, db, user="root", pwd=""):
        return create_engine(f"mysql+pymysql://{user}@{host}:9030/{db}")


def data_to_doris(file_path, day_s, day_e):
    pd_data = pd.DataFrame(columns=Fields)
    s = time.time()
    doris_tar = create_engine(
        "mysql+pymysql://root@10.158.16.244:9030/Salesforce_api_data"
    )
    # ff = (
    #     src_data.replace('"\r "', '"||-||"')
    #     .replace('"|"', '"||-||"')
    #     .replace('"\r\n"', '"||-||"')
    #     .replace('" "00', '"||-||"00')
    #     .replace("\r\n", "")
    # )
    # pd_data["Push_Counter__c"] = pd_data["Push_Counter__c"].apply(
    #             lambda x: x.replace('"', "")
    #         )
    with open(file_path, "r", encoding="utf-8") as f:
        ff = (
            (f.read())
                .replace('"|"', '"||-||"')
                .replace("\n", " ")
                .replace('" "00', '"||-||"00')
        )
        data = ff.split("||-||")
        j = 0
        for i in range(0, len(data), len(Fields)):
            try:
                pd_data.loc[len(pd_data)] = data[i: i + len(Fields)]
            except:
                print(f"request timeout retry day_s {day_s} day_e {day_e}")
                # run(day_s, day_e)
            if len(pd_data) == 1000 or i + len(Fields) == len(data):
                pd_data = pd_data.applymap(lambda x: x[1:-1])
                pd_data.to_sql(
                    "T_" + Table, doris_tar, if_exists="append", index=False
                )
                j = j + len(pd_data)
                print(
                    f"\r{j}  {round(j / (len(data) / len(Fields)), 2) * 100}% done",
                    end="",
                )
                pd_data.drop(pd_data.index, inplace=True)
    print(f" use {round(time.time() - s, 2)}")
    doris_tar.dispose()


def run(arg):
    day_s = arg[0]
    day_e = arg[1]
    sf = SalesForce("stage")
    # doris_tar = create_engine(
    #     "mysql+pymysql://root@10.158.16.244:9030/Salesforce_api_data"
    # )
    # pd_max_id = pd.read_sql(
    #     "select max(id) as id from Salesforce_api_data.T_Opportunity", doris_tar
    # )
    # max_id = str(list(pd_max_id["id"])[0])
    # CreatedDate >= {day_s}T00:00:00Z and CreatedDate < {day_e}T00:00:00Z
    job_id = ""
    fields = ','.join(Fields)
    while job_id == "":
        sf.delete_job(job_id)
        job = sf.create_job(
            f"""
                select
                {fields}
                from {Table} where CreatedDate >= {day_s}T00:00:00Z and CreatedDate < {day_e}T00:00:00Z
            """,
            chunk_size=250000,
            parent="Opportunity",
        )
        job_id = job.get("id")
    print(sf.query_job(job_id))
    print(f"job_id : {job_id} day_s : {day_s} day_e : {day_e}")
    state = ""
    s1 = time.time()
    while state != "JobComplete":
        state = sf.query_job(job_id).get("state")
        print(f"\rstate : {state}  {round(time.time() - s1, 2)} s", end="")
        time.sleep(10)
    s2 = time.time()
    f = sf.download_job(job_id)
    print(f" job has download {round(time.time() - s2, 2)} s")
    os_dir = os.getcwd()
    file_path = f"{os_dir}\\work_daily\\Salesforce\\oppty_{day_s}-{day_e}.txt"
    print(file_path)
    try:
        os.remove(file_path)
    except:
        pass
    with open(
            file_path,
            "w",
            encoding="utf-8",
    ) as fs:
        for line in f.decode("utf-8").split("\n"):
            try:
                fs.write(str(line))
            except Exception as e:
                print(e)
                print(line)
    time.sleep(10)
    if os.path.isfile(file_path):
        # sf.delete_job(job_id)

        s = time.time()
        data_to_doris(file_path, day_s, day_e)
        print(f"job done use {round(time.time() - s, 2)} s")
    else:
        print(f"job file not ready {job_id}")
    # df = pd.read_csv(io.StringIO(f.decode("utf-8")), sep="\t")
    # df.to_csv(".\\user_part.csv", encoding="utf-8")
    # doris_conn = sf.conn_doris(host='10.158.16.244', db='Salesforce_api_data')
    # df.to_sql('User', doris_conn, if_exists='append', index=False)
    # oppty_2019-01-31-2019-02-10 oppty_2019-03-02-2019-03-12


if __name__ == "__main__":
    mon_s = pd.date_range(start="2020-02-16", end="2023-05-01", freq="10D")
    with MyThreadPoolExecutor(max_workers=8) as t:
        for i in mon_s:
            next_day = i + relativedelta(days=10)
            batch_date = i.strftime("%Y-%m-%d")
            next_batch_date = next_day.strftime("%Y-%m-%d")
            # print(batch_date)
            t.submit(run, (batch_date, next_batch_date))
    # data_to_doris(
    #     f"C:\\Users\\Wilson S Wang\\Wilson_data\\97_WorkHome\\pyData\\TestPy\\work_daily\\PyScript\\Salesforce\\API\\Oppty_Line_Item\\txt\\OpportunityLineItem_2019-02-2019-03.txt"
    # )
    # sf = SalesForce("stage")
    # print(sf.query("select id from user limit 10"))
