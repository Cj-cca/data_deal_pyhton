import time
import requests
import base64
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor
from dateutil.relativedelta import relativedelta
import queue
import warnings

# 忽略所有警告
warnings.filterwarnings("ignore")

SelectTable = "Client_Segment__c"
TargetTable = "T_Client_Segment__c"
Fields = ["Id", "IsDeleted", "Name", "CurrencyIsoCode", "CreatedDate", "CreatedById", "LastModifiedDate",
          "LastModifiedById", "SystemModstamp", "LastViewedDate", "LastReferencedDate", "Client__c", "Segment__c",
          "Client_Party_Id__c", "Client_Party_Reference_Id__c", "Security_Territory__c"]

tarEngine = create_engine('mysql+pymysql://root@10.158.16.244:9030/Salesforce_api_data')
AllDataCount = 0


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
            self.content_type = "application/xml"

        elif env == "preview":
            self.url = (
                "https://ddei3-0-ctp.asiainfo-sec.com:443/wis/clicktime/v1/query?"
                "url=https%3a%2f%2fpwcedcrm.glb%2dcrm%2dpreview233.pwcstg.myshn.net%"
                "2f&umid=C7FAC8A5-FBEA-B305-AA54-87E91DECB7F5&"
                "auth=1b7026dd0d0ddf2d048eb69ec2055ced11f64474-6a2545001d8ebbb6a471d7a869e12a94c8e93322"
            )
            self.user_name = "warehousedata.chk@pos.eu.preview"
            self.password = "f2waxpPSgMZEwDXeYsxR"
            self.token = "Z0VgyaOqmnMKPgJvdcrYaIkz"
            self.consumer_key = "3MVG904d7VkkD2aOUQbHyNOKwtafecCePjqUCIRTb9DA1KrhMxCWKP_9mWyyhOSKR8oCglnCvF1Dr0LH_FGQq"
            self.consumer_secret = (
                "CFDE03395021F84BD9C3F8184F2450184654E27E2D071B83377FECCA59E5D8CA"
            )
            self.SFDC_Domain = "Preview233"

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

    def create_job(self, q, chunk_size=None, parent=None):
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
            "columnDelimiter": "TAB",
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


def readDataToDoris():
    global AllDataCount
    for v in range(0, 60):
        queryDateStart = f"2020-03-23T06:{str(v).zfill(2)}:00Z"
        if v == 59:
            queryDateEnd = f"2020-03-23T07:00:00Z"
        else:
            queryDateEnd = f"2020-03-23T06:{str(v + 1).zfill(2)}:00Z"
        tmpStart = queryDateStart
        result_df = pd.DataFrame(columns=Fields)
        selectCount = 200
        totalCount = 0
        print(f"{tmpStart}数据请求开始")
        s1 = time.time()
        json = ""
        try:
            while selectCount >= 200:
                json = sf.query(
                    f"select fields(all) from {SelectTable} where CreatedDate >= {queryDateStart} and "
                    f"CreatedDate < {queryDateEnd} order  by CreatedDate limit 200 "
                )
                selectCount = int(json["totalSize"])
                totalCount += selectCount
                if selectCount == 0:
                    break
                if 'records' not in json:
                    print(f"{tmpStart}请求数据异常: ", json)
                    return
                tmp_df = pd.json_normalize(json['records'], max_level=0)
                queryDateStart = tmp_df['CreatedDate'].max()
                del tmp_df['attributes']
                result_df = pd.concat([result_df, tmp_df], ignore_index=True)
            s2 = time.time()
            print(f"{tmpStart}数据请求结束，总共{totalCount}条数据，用时{s2 - s1}s，开始入数据库")
            if totalCount == 0:
                continue
            result_df = result_df.fillna('').astype(str)
            insert_count = result_df.to_sql(TargetTable, tarEngine, if_exists='append', index=False)
            if insert_count != totalCount:
                print(f"{tmpStart}-{queryDateEnd}数据插入不完整")
            AllDataCount += insert_count
            print(f"{tmpStart}-{queryDateEnd}数据插入成功，受影响行数：", insert_count)
        except Exception as e:
            print(f"{tmpStart}-{queryDateEnd}数据同步异常: ", e, json)


if __name__ == "__main__":
    sf = SalesForce("stage")
    readDataToDoris()
    # mon_s = pd.date_range(start="2020-03-22", end="2020-03-24", freq="1D")
    # for i in mon_s[:-1]:
    #     next_day = i + relativedelta(days=1)
    #     batch_date = i.strftime("%Y-%m-%d")
    #     next_batch_date = next_day.strftime("%Y-%m-%d")
    #     # print(batch_date)
    #     readDataToDoris((batch_date, next_batch_date))
    # with MyThreadPoolExecutor(max_workers=3) as t:
    #     for i in mon_s:
    #         next_day = i + relativedelta(days=10)
    #         batch_date = i.strftime("%Y-%m-%d")
    #         next_batch_date = next_day.strftime("%Y-%m-%d")
    #         # print(batch_date)
    #         t.submit(readDataToDoris, (batch_date, next_batch_date))
    print(f"最终插入数据条数: {AllDataCount}")
    # mon_s = pd.date_range(start="2020-03-16", end="2023-05-01", freq="10D")
    # for i in mon_s:
    #     next_day = i + relativedelta(days=10)
    #     batch_date = i.strftime("%Y-%m-%d")
    #     next_batch_date = next_day.strftime("%Y-%m-%d")
    # print(batch_date)
    # run(batch_date, next_batch_date)
