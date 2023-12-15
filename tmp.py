# !/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas
import requests
import urllib.request
import base64
import json


def getToken(url, user, pwd):
    credentials = "{user}:{pwd}".format(user=user, pwd=pwd)
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    auth = f"Basic {encoded_credentials}"
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
        "Authorization": auth
    }
    response = requests.get(url, headers=headers, verify=False)
    if response.status_code == 200:
        data_frame = pandas.json_normalize(response.json())
        result = data_frame.get(["token_type", "access_token"])
        return ' '.join(result.values.tolist()[0])
    else:
        print(response)
        return ''


def getTokenNew(url, user, pwd):
    credentials = "{user}:{pwd}".format(user=user, pwd=pwd)
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    auth = f"Basic {encoded_credentials}"
    headers = {
        "Authorization": auth
    }
    req = urllib.request.Request(url, headers=headers)
    response = urllib.request.urlopen(req)
    if response.code == 200:
        data = response.read()
        data_frame = pandas.json_normalize(json.loads(data.decode('utf-8')))
        result = data_frame.get(["token_type", "access_token"])
        return ' '.join(result.values.tolist()[0])
    else:
        print(response)
        return ''


def get_api_data_count(query_url, token):
    global avgCostTime, maxCostTime
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
        "Authorization": token
    }
    proxies = requests.utils.getproxies()
    if proxies and 'https' in proxies:
        proxies['https'] = proxies['http']
    response = requests.get(query_url, headers=headers, proxies=proxies, verify=False).json()
    return int(response)


if __name__ == '__main__':
    getTokenUrl = "https://pwc.it-cpi010-rt.cpi.cn40.apps.platform.sapcloud.cn/http/vprofile/gettoken"
    # getTokenUrl = "https://pwc-dev.it-cpi010-rt.cpi.cn40.apps.platform.sapcloud.cn/http/vProfile/gettoken"
    # getDataCountUrl = "https://api15.sapsf.cn/odata/v2/PerPerson/$count"
    getDataCountUrl = "https://api15.sapsf.cn/odata/v2/PerPerson/$count"
    # user = "sb-a8531244-374b-414c-8944-0dbdf941c2e5!b1813|it-rt-pwc-dev!b39"
    user = "sb-9e4a42e7-4439-4782-95ce-a149c045c26e!b2390|it-rt-pwc!b39"
    pwd = "9732d1fd-2fb1-4080-97cb-cd82df084219$-BDmkDUlmMek7Dj9bS5w7Tqlzwdm7o2XIi5tPZaGMwQ="
    # pwd = "f4aee3e5-9539-4568-be98-404a5c6ca253$yxW2FNy_fKA8a1Fjn44SM3zjSt4VGvIbzu9tQnHfWdg="
    requestToken = getTokenNew(getTokenUrl, user, pwd)
    print(requestToken)
    if requestToken != '':
        count = get_api_data_count(getDataCountUrl, requestToken)
        print(count)
    else:
        print("exception：requestToken get failed")
