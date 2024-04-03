import re
import asyncio

import httpx
from bs4 import BeautifulSoup

lv1_url_reference = "https://referensi.data.kemdikbud.go.id/pendidikan/dikdas"


def get_list_prov(target_url:str=lv1_url_reference, cust_header:dict=None, verify:bool=False):
    client = httpx.Client(headers=cust_header, verify=verify)
    raw_resp = client.get(target_url).content
    raw_data = BeautifulSoup(raw_resp, features='lxml').find_all('tbody')[0]
    data = []
    for row in raw_data.findAll('tr'):
        tab_data = row.findAll('td')[1].find('a')
        nomenklatur = tab_data.contents[0]
        dikbud_id = re.search(pattern=r"\d{6,8}", string=tab_data.get('href')).group()
        _data = {
            'province': nomenklatur.strip(),
            'dikbud_id': dikbud_id,
            }
        data.append(_data)
    return data

async def get_list_reg(province:dict, cust_header:dict=None, verify:bool=False):
    async with httpx.AsyncClient(headers=cust_header, verify=verify, timeout=None) as client:
        raw_resp = await client.get(f"{lv1_url_reference}/{province['dikbud_id']}/1")
        _content = raw_resp.content
        raw_data = BeautifulSoup(_content, features='lxml').find_all('tbody')[0]
        data = []
        for row in raw_data.findAll('tr'):
            tab_data = row.findAll('td')[1].find('a')
            nomenklatur = tab_data.contents[0]
            dikbud_id = re.search(pattern=r"\d{6,8}", string=tab_data.get('href')).group()
            _data = {
                'province': province['province'],
                'dikbud_id': province['dikbud_id'],
                'city_reg': nomenklatur.strip(),
                'dikbud_id2': dikbud_id,
                }
            data.append(_data)
        return data

async def get_list_subreg(city_reg:dict, cust_header:dict=None, verify:bool=False):
    async with httpx.AsyncClient(headers=cust_header, verify=verify, timeout=None) as client:
        raw_resp = await client.get(f"{lv1_url_reference}/{city_reg['dikbud_id2']}/2")
        _content = raw_resp.content
        raw_data = BeautifulSoup(_content, features='lxml').find_all('tbody')[0]
        data = []
        for row in raw_data.findAll('tr'):
            tab_data = row.findAll('td')[1].find('a')
            nomenklatur = tab_data.contents[0]
            dikbud_id = re.search(pattern=r"\d{6,8}", string=tab_data.get('href')).group()
            _data = {
                'province': city_reg['province'],
                'dikbud_id': city_reg['dikbud_id'],
                'city_reg': city_reg['city_reg'],
                'dikbud_id2': city_reg['dikbud_id2'],
                'district': nomenklatur.strip(),
                'dikbud_id3': dikbud_id,
                }
            data.append(_data)
        return data

def update_adm_list(cust_header:dict = None):
    base_list = get_list_prov(cust_header=cust_header)
    get_reg_job  = [get_list_reg(province=prov, cust_header=cust_header) for prov in base_list]
    reg_list_exec = asyncio.run(asyncio.gather(*get_reg_job))
    reg_list = [element for sub_list in reg_list_exec for element in sub_list]
    get_subreg_job = [get_list_subreg(city_reg=reg, cust_header=cust_header) for reg in reg_list]
    subreg_list_exec = asyncio.run(asyncio.gather(*get_subreg_job))
    subreg_list = [element for sub_list in subreg_list_exec for element in sub_list]
    return subreg_list

