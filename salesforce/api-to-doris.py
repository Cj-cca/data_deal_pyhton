import time
import requests
import base64
import os
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import pandas as pd
import io

Table = "Connected_Opportunity__c"
TargetTable = "t_connected_opportunity__c"
Fields = ["Id", "IsDeleted", "Name", "CurrencyIsoCode", "CreatedDate", "CreatedById", "LastModifiedDate",
          "LastModifiedById", "SystemModstamp", "Opportunity__c", "Connected_Opportunity__c", "Connection_Type__c",
          "Relationship__c", "Ultimate_Parent_Opportunity__c"]


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
            except Exception as e:
                print(f"day_s {day_s} day_e {day_e} to doris fail, {e}")
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
    print(f"job done, use {round(time.time() - s, 2)}")
    doris_tar.dispose()


def data_to_doris_new(file_path, day_s, day_e):
    pd_data = pd.DataFrame(columns=Fields)
    s = time.time()
    doris_tar = create_engine(
        "mysql+pymysql://root@10.158.16.244:9030/Salesforce_api_data"
    )
    with open(file_path, "r", encoding="utf-8") as f:
        f.readline()
        lines = f.readlines()
        for line in lines:
            pd_data.loc[len(pd_data)] = line.replace('\n', '').strip('"').split('"|"')
            if len(pd_data) == 10000:
                pd_data.to_sql(
                    TargetTable, doris_tar, if_exists="append", index=False
                )
                pd_data.drop(pd_data.index, inplace=True)
        if len(pd_data) != 0:
            pd_data.to_sql(
                "T_" + Table, doris_tar, if_exists="append", index=False
            )
        print(f" use {round(time.time() - s, 2)}")
        doris_tar.dispose()


def run(day_s, day_e):
    sf = SalesForce("stage")
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
            parent=Table,
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

    try:
        pd_data = pd.DataFrame(columns=Fields)
        s = time.time()
        doris_tar = create_engine(
            "mysql+pymysql://root@10.158.16.244:9030/Salesforce_api_data"
        )
        insertCount = 0
        lines = f.decode("utf-8").split("\n")
        for line in lines[1:-1]:
            pd_data.loc[len(pd_data)] = line.replace('\r', '')[1:-1].split('"|"')
            if len(pd_data) == 10000:
                insertCount += pd_data.to_sql(
                    TargetTable, doris_tar, if_exists="append", index=False
                )
                pd_data.drop(pd_data.index, inplace=True)

        if len(pd_data) != 0:
            insertCount += pd_data.to_sql(
                TargetTable, doris_tar, if_exists="append", index=False
            )
        print(
            f"{day_e}-{day_e} job done, use {round(time.time() - s, 2)}, dataCount = {len(lines) - 2}, insertCount = {insertCount}")
        doris_tar.dispose()
    except Exception as e:
        print(f"except: {e}")


def run_hour():
    day_s = '2020-03-23'
    day_e = '2020-03-23'
    for v in range(2, 60):
        queryDateStart = f"{day_s}T06:{str(v).zfill(2)}:00Z"
        if v == 59:
            queryDateEnd = f"{day_e}T07:00:00Z"
        else:
            queryDateEnd = f"{day_e}T06:{str(v + 1).zfill(2)}:00Z"
        sf = SalesForce("stage")
        job_id = ""
        fields = ','.join(Fields)
        while job_id == "":
            sf.delete_job(job_id)
            job = sf.create_job(
                f"""
                    select
                    {fields}
                    from {Table} where CreatedDate >= {queryDateStart} and CreatedDate < {queryDateEnd}
                """,
                chunk_size=250000,
                parent=Table,
            )
            job_id = job.get("id")
        print(sf.query_job(job_id))
        print(f"job_id : {job_id} day_s : {queryDateStart} day_e : {queryDateEnd}")
        state = ""
        s1 = time.time()
        while state != "JobComplete":
            state = sf.query_job(job_id).get("state")
            print(f"\rstate : {state}  {round(time.time() - s1, 2)} s", end="")
            time.sleep(10)
        s2 = time.time()
        f = sf.download_job(job_id)
        sf.delete_job(job_id)
        print(f" job has download {round(time.time() - s2, 2)} s")

        try:
            pd_data = pd.DataFrame(columns=Fields)
            s = time.time()
            doris_tar = create_engine(
                "mysql+pymysql://root@10.158.16.244:9030/Salesforce_api_data"
            )
            insertCount = 0
            lines = f.decode("utf-8").split("\n")
            # for line in lines[1:-1]:
            #     pd_data.loc[len(pd_data)] = line.replace('\r', '').strip('"').split('"|"')
            #     if len(pd_data) == 10000:
            #         insertCount += pd_data.to_sql(
            #             "T_" + Table, doris_tar, if_exists="append", index=False
            #         )
            #         pd_data.drop(pd_data.index, inplace=True)
            #
            # if len(pd_data) != 0:
            #     insertCount += pd_data.to_sql(
            #         "T_" + Table, doris_tar, if_exists="append", index=False
            #     )
            # print(f"{queryDateStart}-{queryDateEnd} job done, use {round(time.time() - s, 2)}, dataCount = {insertCount}")
            # doris_tar.dispose()
        except Exception as e:
            print(f"except: {e}")


def run_new(day_s, day_e):
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
            parent=Table,
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
    file_path = f"C:\\Users\\Austin J Cheng\\PycharmProjects\\pythonProject\\work_daily\\Salesforce\\oppty_{day_s}-{day_e}.txt"
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
        data_to_doris_new(file_path, day_s, day_e)
        print(f"job done use {round(time.time() - s, 2)} s")
    else:
        print(f"job file not ready {job_id}")
    # df = pd.read_csv(io.StringIO(f.decode("utf-8")), sep="\t")
    # df.to_csv(".\\user_part.csv", encoding="utf-8")
    # doris_conn = sf.conn_doris(host='10.158.16.244', db='Salesforce_api_data')
    # df.to_sql('User', doris_conn, if_exists='append', index=False)
    # oppty_2019-01-31-2019-02-10 oppty_2019-03-02-2019-03-12


if __name__ == "__main__":
    mon_s = pd.date_range(start="2019-01-01", end="2023-6-5", freq="5D")
    for i in mon_s[:-1]:
        next_day = i + relativedelta(days=5)
        batch_date = i.strftime("%Y-%m-%d")
        next_batch_date = next_day.strftime("%Y-%m-%d")
        # print(batch_date)
        run(batch_date, next_batch_date)
    # data_to_doris(
    #     f"C:\\Users\\Wilson S Wang\\Wilson_data\\97_WorkHome\\pyData\\TestPy\\work_daily\\PyScript\\Salesforce\\API\\Oppty_Line_Item\\txt\\OpportunityLineItem_2019-02-2019-03.txt"
    # )
    # sf = SalesForce("stage")
    # print(sf.query("select id from user limit 10"))
