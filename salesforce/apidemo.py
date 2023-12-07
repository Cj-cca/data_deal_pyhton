import time
import requests
import base64
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import io


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


if __name__ == "__main__":
    sf = SalesForce("stage")
    json = sf.query(
        "select fields(all) from Task limit 1"
    )
    # json = sf.query(
    #     "select count(Id) from OpportunityTeamMember"
    # )
    # for v in range(2, 60):
    #     s = f"2020-03-23T06:{str(v).zfill(2)}:00Z"
    #     if v == 59:
    #         e = f"2020-03-23T07:00:00Z"
    #     else:
    #         e = f"2020-03-23T06:{str(v + 1).zfill(2)}:00Z"
    #     json = sf.query(
    #         f"select fields(all) from Client_Segment__c where CreatedDate>={s} and CreatedDate<{e} limit 200"
    #     )
    #     if 'records' in json:
    #         print(f"{s}={e}", json['records'])
    if 'records' not in json:
        print(json)
    tablePrefix = 'CREATE TABLE `t_account` ( \n'
    fieldSuffix = ' varchar(256) NULL COMMENT "",'
    tableSuffix = ''') ENGINE=OLAP
        UNIQUE KEY(`Id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`Id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );'''
    data = json['records'][0]
    print("origin keys:", len(json['records'][0].keys()))
    fields = []
    fieldsAll = []
    createTable = tablePrefix
    for key in data:
        fieldLine = '\t`' + key + '`' + fieldSuffix + '\n'
        createTable = createTable + fieldLine
        fieldsAll.append(key)
        if not isinstance(data[key], dict):
            fields.append(key)
        else:
            print(key)
    createTable = createTable + tableSuffix
    print("fields: ", '","'.join(fields))
    print("all fields: ", '","'.join(fieldsAll))
    print("table sql, ", createTable)
    print("first Data Date: ", json['records'][0]['CreatedDate'])