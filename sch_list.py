import re
import asyncio
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

__base_url = "https://referensi.data.kemdikbud.go.id/pendidikan"

direktorat = ['paud', 'dikdas', 'dikmen']

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
            try:
                address = column[3].contents[0]
            except IndexError:
                address = None
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
    
async def get_sch_detail(sch_meta:dict, cust_header:dict=None, verify:bool=False):
    async with httpx.AsyncClient(headers=cust_header, verify=verify, timeout=None) as client:
        raw_resp = await client.get(f"{sch_meta['link']}")
        _content = raw_resp.content
        raw_data = BeautifulSoup(_content, features='lxml').find_all(name='div',attrs={'class': 'tabby-content'})
        # raw_resp.aclose()
        try:    
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
            try:
                i_serv = re.split(r", | dan ", _identity[10].findAll('td')[3].contents[0])
                i_services = [prog for prog in i_serv if prog != '']
            except IndexError:
                i_services = None
            _legal = raw_data[1].contents[1].findAll('tr')
            l_ministry_spv = _legal[0].findAll('td')[3].contents[0]

            try:
                l_inst_spv = _legal[1].findAll('td')[3].contents[0]
            except IndexError:
                l_inst_spv = None
            
            try:
                l_foundation = _legal[2].findAll('td')[3].contents[1]
                l_fnd_id = l_foundation.contents[0].strip()
                l_fnd_detail = l_foundation.get('href')
            except IndexError:
                l_fnd_id = None
                l_fnd_detail = None
            
            try:
                l_establish_lt = _legal[3].findAll('td')[3].contents[0]
            except IndexError:
                l_establish_lt = None
            
            try:
                l_establish_date = datetime.strptime(_legal[4].findAll('td')[3].contents[0], "%d-%m-%Y").date()
            except ValueError:
                l_establish_date = None
            try:
                l_opr_id = _legal[5].findAll('td')[3].contents[0]
            except IndexError:
                l_opr_id = None
            
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
                c_email = _contact[2].findAll('td')[3].contents[0]
            except IndexError:
                c_email = None
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
                'pr_layanan': i_services,
                'status': i_status,
                'kementerian': l_ministry_spv,
                'naungan': l_inst_spv,
                'npyp': l_fnd_id,
                'detail_yayasan': l_fnd_detail,
                'no_sk_pendirian': l_establish_lt,
                'tgl_sk_pendirian': l_establish_date.isoformat(),
                'no_sk_ijop': l_opr_id,
                'tgl_sk_ijop': l_opr_date.isoformat(),
                'tautan_sk_ijop': l_opr_lt_link,
                'tgl_unggah_ijop': l_opr_lt_up_date.isoformat(),
                'website': c_website,
                'email': c_email,
                'tautan_skita': i_skita_link,
                'induk_kec': sch_meta['induk_kec'],
                'bujur': longitude,
                'lintang': latitude,
                }
        except IndexError:
            data = sch_meta
            data['error'] = 'detail_not_found'

        return data

# def __paginator(items:list, items_per_page:int):
#     pages = [items[item:item+items_per_page] for item in range(0, len(items), items_per_page)]
#     return pages
    
# def get_dataset(subdistricts:list, w_units:list, get_detail:bool, batch_limit:int=400, cust_header:dict=None, ssl_verif:bool=False) -> dict:
#     permut_target = [{'subdist': subdist, 'w_unit': w_unit} for subdist in subdistricts for w_unit in w_units]
#     __sch_list = []
#     for subset in __paginator(permut_target, batch_limit):
#         crawl_list = [get_sch_list(permut['subdist'], permut['w_unit'], cust_header, ssl_verif) for permut in subset]
#         data_list = asyncio.run(asyncio.gather(*crawl_list))
#         __sch_list += data_list
#     sch_list = [element for sublist in __sch_list for element in sublist]
#     match get_detail:
#         case False:
#             return sch_list
#         case True:
#             # throttle = __paginator(sch_list, batch_limit)
#             data = []
#             for subset in __paginator(sch_list, batch_limit):
#                 crawl_detail = [get_sch_detail(sch, cust_header=cust_header) for sch in subset]
#                 detail_info = asyncio.run(asyncio.gather(*crawl_detail))
#                 data += detail_info
#             return data
