import re
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

__base_url = "https://referensi.data.kemdikbud.go.id/pendidikan"

direktorat = ['paud', 'dikdas', 'dikmen', 'dikmas']

async def get_sch_list(dikbud_id3:str, direktorat:str, cust_header:dict=None, verify:bool=False):
    async with httpx.AsyncClient(headers=cust_header, verify=verify, timeout=None) as client:
        raw_resp = await client.get(f"{__base_url}/{direktorat}/{dikbud_id3}/3")
        _content = raw_resp.content
        raw_data = BeautifulSoup(_content, features='lxml').find_all('tbody')[0]
        data = []
        for row in raw_data.findAll('tr'):
            column = row.findAll('td')
            basic_id = column[1].find('a')
            npsn = basic_id.contents[0]
            link = basic_id.get('href')
            parent_district = dikbud_id3
            inst_name = column[2].contents[0]
            address = column[3].contents[0]
            subdistrict = column[4].contents[0]
            status = column[5].contents[0]
            _data = {
                'npsn': npsn,
                'nama_lembaga': inst_name,
                'alamat': address,
                'kelurahan': subdistrict,
                'induk_kec': parent_district,
                'status': status,
                'link': link
                }
            data.append(_data)

        return data
    
async def get_sch_detail(sch_meta:dict, cust_header:dict=None, verify:bool=None):
    async with httpx.AsyncClient(headers=cust_header, verify=verify, timeout=None) as client:
        raw_resp = await client.get(f"{sch_meta['link']}")
        _content = raw_resp.content
        raw_data = BeautifulSoup(_content, features='lxml').find_all(name='div',attrs={'class': 'tabby-content'})
        _identity = raw_data[0].contents[1].findAll('tr')
        i_name = _identity[0].findAll('td')[3].contents[0]
        i_id = _identity[1].findAll('td')[3].find('a').contents[0]
        i_skita_link = _identity[1].findAll('td')[3].find('a').get('href')
        i_address = _identity[2].findAll('td')[3].contents[0]
        i_subdistrict = _identity[4].findAll('td')[3].contents[0]
        i_district = _identity[5].findAll('td')[3].contents[0]
        i_city_reg = _identity[6].findAll('td')[3].contents[0]
        i_province = _identity[7].findAll('td')[3].contents[0]
        i_status = _identity[8].findAll('td')[3].contents[0]
        i_inst_type = _identity[9].findAll('td')[3].contents[0]
        _legal = raw_data[1].contents[1].findAll('tr')
        l_ministry_spv = _legal[0].findAll('td')[3].contents[0]

        try:
            l_inst_spv = _legal[1].findAll('td')[3].contents[0]
        except IndexError:
            l_inst_spv = None

        l_inst_id = _legal[2].findAll('td')[3].contents[0]
        try:
            l_foundation_lt = _legal[3].findAll('td')[3].contents[0]
        except IndexError:
            l_foundation_lt = None
        try:
            l_foundation_date = datetime.strptime(_legal[4].findAll('td')[3].contents[0], "%d-%m-%Y").date()
        except ValueError:
            l_foundation_date = None
        l_opr_id = _legal[5].findAll('td')[3].contents[0]
        try:
            l_opr_date = datetime.strptime(_legal[6].findAll('td')[3].contents[0], "%d-%m-%Y").date()
        except:
            l_opr_date = None
        l_opr_lt = _legal[7].findAll('td')[3].contents[1]

        if l_opr_lt.contents[0] != "Silakan Upload SK (link file tidak valid)":
            l_opr_lt_link = l_opr_lt.get('href')
        else:
            l_opr_lt_link = None
        try:
            l_opr_lt_up_date = datetime.strptime(_legal[8].findAll('td')[3].contents[0].strip(), "%Y-%m-%d %H:%M:%S.%f").date()
        except ValueError:
            l_opr_lt_up_date = None
        _contact = raw_data[3].contents[1].findAll('tr')
        try:
            c_website = _contact[3].findAll('td')[3].contents[0].get('href')
        except IndexError:
            c_website = None
        _coordinate = raw_data[4].contents[1].findAll('div')[2].contents
        long = abs(float(re.search(pattern=r"(?<=Bujur:\s).*", string=_coordinate[2]).group()))
        if long != 0:
            longitude = long
        else:
            longitude = None
        lat = abs(float(re.search(pattern=r"(?<=Lintang:\s).*", string=_coordinate[0]).group()))*-1
        if lat != 0:
            latitude = lat
        else:
            latitude = None
        data = {
            'npsn': i_id,    
            'nama_lembaga': i_name,
            'alamat':i_address,
            'kelurahan': i_subdistrict,
            'kecamatan': i_district,
            'kab_kota': i_city_reg,
            'provinsi': i_province,
            'btk_pend': i_inst_type,
            'status': i_status,
            'kementerian': l_ministry_spv,
            'naungan': l_inst_spv,
            'npyp': l_inst_id,
            'no_sk_pendirian': l_foundation_lt,
            'tgl_sk_pendirian': l_foundation_date,
            'no_sk_ijop': l_opr_id,
            'tgl_sk_ijop': l_opr_date,
            'tautan_sk_ijop': l_opr_lt_link,
            'tgl_unggah_ijop': l_opr_lt_up_date,
            'website': c_website,
            'tautan_skita': i_skita_link,
            'induk_kec': sch_meta['induk_kec'],
            'bujur': longitude,
            'lintang': latitude,
            }

        return data
