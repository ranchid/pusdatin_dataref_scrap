import json
import logging
import sys
import asyncio
import json


from sch_list import get_sch_list, get_sch_detail
logstream_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(handlers=[logstream_handler],
                    format='%(asctime)s.%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)
with open(file='adm_list.json', mode='r') as f:
    data = f.read()

cust_header = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'User-Agent': "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0"
    }

teritori = json.loads(data)

sel_teritori = [unit['dikbud_id3'] for unit in teritori if unit['province'] == 'Jawa Tengah' and unit['city_reg'] == 'Kab. Jepara']

target_dir = ['paud', 'dikdas', 'dikmen']

async def job_aggregator(job_list):
    job_agg = await asyncio.gather(*job_list)
    return job_agg

def __paginator(items:list, items_per_page:int):
    pages = [items[item:item+items_per_page] for item in range(0, len(items), items_per_page)]
    return pages

    
def get_dataset(subdistricts:list, w_units:list, get_detail:bool, batch_limit:int=100, cust_header:dict=None, ssl_verif:bool=False) -> dict:
    permut_target = [{'subdist': subdist, 'w_unit': w_unit} for subdist in subdistricts for w_unit in w_units]
    data_session =
    def __get_preliminary():
        with open('tempdata.json', mode='a') as f:
                f.write('[')
                for subset in __paginator(permut_target, batch_limit):
                    crawl_list = [get_sch_list(permut['subdist'], permut['w_unit'], cust_header, ssl_verif) for permut in subset]
                    data_list = asyncio.run(job_aggregator(crawl_list))
                    for datum_array in data_list:
                        for datum in datum_array:
                            f.write(json.dumps(datum))
                            f.write(',\n')
                
                #remove 2 last characters
                f.truncate(f.seek(0,2)-2)
                
                f.write(f"]")
                f.close()

    match get_detail:
        case False:
            with open('tempdata.json', mode='a') as f:
                f.write('[')
                for subset in __paginator(permut_target, batch_limit):
                    crawl_list = [get_sch_list(permut['subdist'], permut['w_unit'], cust_header, ssl_verif) for permut in subset]
                    data_list = asyncio.run(job_aggregator(crawl_list))
                    for datum_array in data_list:
                        for datum in datum_array:
                            f.write(json.dumps(datum))
                            f.write(',\n')
                
                #remove 2 last characters
                f.truncate(f.seek(0,2)-2)
                
                f.write(f"]")
                f.close()


    # __sch_list = []
    # for subset in __paginator(permut_target, batch_limit):
    #     crawl_list = [get_sch_list(permut['subdist'], permut['w_unit'], cust_header, ssl_verif) for permut in subset]
    #     data_list = asyncio.run(job_aggregator(crawl_list))
    #     __sch_list += data_list
    # sch_list = [element for sublist in __sch_list for element in sublist]
    # match get_detail:
    #     case False:
    #         return sch_list
    #     case True:
    #         # throttle = __paginator(sch_list, batch_limit)
    #         data = []
    #         for subset in __paginator(sch_list, batch_limit):
    #             crawl_detail = [get_sch_detail(sch, cust_header=cust_header) for sch in subset]
    #             detail_info = asyncio.run(job_aggregator(crawl_detail))
    #             data += detail_info
    #         return data


if __name__ == "__main__":
    get_dataset(sel_teritori, target_dir, get_detail=False, cust_header=cust_header)
    # coba = get_dataset(sel_teritori, target_dir, get_detail=False, cust_header=cust_header)
    # with open("tempdata.json", mode="w") as f:
    #     f.write(json.dumps(coba))
    #     print('done')
    #     f.close()