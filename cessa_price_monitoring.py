import os
import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.utils.trigger_rule import TriggerRule


local_tz = pendulum.timezone("Asia/Jakarta")


local_tz = pendulum.timezone("Asia/Jakarta")
dag_id = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
schedule = Variable.get(
    f'schedule_{dag_id}', 
    default_var={'start_date':'2022-06-17', 'schedule_interval':None},
    deserialize_json=True
)

@dag(
    dag_id,
    start_date = pendulum.parse(schedule['start_date'], tz=local_tz),
    schedule_interval = schedule['schedule_interval'], # Daily at 0.30 AM UTC+7
    catchup = False,
    default_args = {
        "depends_on_past": False,
        "owner" : 'akbar',
        "retries": 3,
        "retry_delay": timedelta(minutes=8),
        "email": ['akbar@hypefast.id'],
        "email_on_failure": False,
        "is_paused_upon_creation":True
    },
    params={
        "date_from":"2022-06-01 00:00:00",
        "date_to":"2023-01-01 00:00:00",
        "time_delta": 1,
        "force_all":False,
    }
)



def cessa_price_monitoring():
    from modules.ScrapingAPI import ShopeeScraper
    from fuzzywuzzy import fuzz
    from datetime import datetime,timezone
    import pandas as pd
    from functools import reduce

    from common.helper_function import bq_query,write_json_to_bq

    proxy_url = "http://L9UNCDF_Q5JqpXtoVAv0lQ:@smartproxy.crawlbase.com:8012"
    proxies = {"http": proxy_url, "https": proxy_url}
    shopee_crawler = ShopeeScraper(origin='shopee.co.id', proxies=proxies)
    keyword = "Cessa Baby Oil"


    def find_similar_products(products, target_name, threshold=70):
        similar_products = []
        for product in products:
            similarity_ratio = fuzz.partial_ratio(product['product_name'].lower(), target_name.lower())
            if similarity_ratio >= threshold:
                similar_products.append(product)
        
        return similar_products
    

    get_cuurent_time = lambda: datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


    def transform_raw_data(data_list:list) -> list:
        results_data = {}

        all_results_data = []

        if not isinstance(data_list,list):
            data_list = [data_list]
    
        convert_to_datetime = lambda ts: datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        for data in data_list:

            if data is not None:
                if 'items' in data and data['items'] is not None:
                    for item in data['items']:
                        try:
                            data_dict = {}
                            product_id = item["item_basic"]["itemid"]
                            shop_id = item["item_basic"]["shopid"]
                            original_price =item["item_basic"]["price"]/100000 if item["item_basic"]["raw_discount"] == 0 else item['item_basic']['price_before_discount']/100000
                            try:
                                percentage_discount = float(item["item_basic"]["raw_discount"])
                            except:
                                percentage_discount = 0

                            if item['item_basic']['tier_variations']:
                                total_variant = reduce(lambda x, y: x * (len(y.get('options', [])) or 1), item['item_basic']['tier_variations'], 1)
                            else:
                                total_variant = 0                

                            wrapper = {
                                "product_id":str(product_id),
                                "product_name":item["item_basic"]["name"],
                                "original_price":int(original_price),
                                "sales_price":int(item['item_basic']['price']/100000),
                                "discount_percentage":round(percentage_discount, 1),
                                "total_product_sold":item['item_basic']['historical_sold'],
                                "shop_url":f"https://www.shopee.co.id/shop/{shop_id}",
                                "shop_id":shop_id,
                                "product_rating" : float(item['item_basic']['item_rating']['rating_star']),
                                "product_image_url" : f"https://down-id.img.susercontent.com/file/{item['item_basic']['image']}",
                                "product_url":str(f"https://www.shopee.co.id/product/{shop_id}/{product_id}"),
                                "product_created_at" : convert_to_datetime(item['item_basic']['ctime']),
                                "total_variant" : total_variant,
                                "product_max_sales_price" : int(item['item_basic']['price_max'] / 100000),
                                "product_min_sales_price" : int(item['item_basic']['price_min'] / 100000),
                                'api_products': f'https://shopee.co.id/api/v4/pdp/get_pc?shop_id={shop_id}&item_id={product_id}&detail_level=0',
                                'ingestion_time' : get_cuurent_time()

                            }

                            data_dict.update(wrapper)

                            all_results_data.append(data_dict)

                        except Exception as e:
                            print(f"Error occurred: {str(e)}")
                            raise 
                


                else:
                    continue
        results_data['product_list'] = all_results_data
        # print("results_data!!!!!!",results_data)
        return results_data









    @task
    def extract_current_shop_products(**kwargs):
        from google.cloud import bigquery


        def upsert_to_bq(new_data_df, target_table_id, staging_table_id):
            client = bigquery.Client()

            # Load new data to staging table
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            load_job = client.load_table_from_dataframe(
                new_data_df, staging_table_id, job_config=job_config
            )
            load_job.result()  # Wait for the job to complete
            print(f"Loaded {load_job.output_rows} rows to {staging_table_id}")

            # Perform the merge to upsert data
            merge_query = f"""
            MERGE `{target_table_id}` T
            USING `{staging_table_id}` S
            ON T.shop_id = S.shop_id
            WHEN MATCHED THEN
            UPDATE SET
                T.shop_url = S.shop_url,
                T.ingestion_time = S.ingestion_time,
                T.shop_name = S.shop_name
            WHEN NOT MATCHED THEN
            INSERT (shop_url, shop_id, ingestion_time, shop_name)
            VALUES (S.shop_url, S.shop_id, S.ingestion_time, S.shop_name)
            """
            query_job = client.query(merge_query)
            query_job.result()  # Wait for the job to complete
            print("Merge job completed")



        results = shopee_crawler.crawl_by_search(keyword=keyword,sort="1",limit=60,count=30)

        print("RESULTS :",results)
        data_list = transform_raw_data(results)
        data = data_list['product_list']
        data = find_similar_products(data, keyword)


        current_data = []


        for data in data:
            result_dict = {}

            result_dict['shop_url'] = str(data['shop_url'])
            result_dict['shop_id'] = str(data['shop_id'])
            result_dict['shop_name'] = ""
            result_dict['ingestion_time'] = get_cuurent_time()


            current_data.append(result_dict)



        CESSA_SHOP_ID_QUERY = """
        SELECT shop_url, shop_id,  DATE(ingestion_time) as ingestion_time , shop_name
        FROM cessa_price_monitoring.master_shop_id
        """


        new_data_master_shop_id = pd.DataFrame(current_data)
        old_data_master_shop_id = bq_query(CESSA_SHOP_ID_QUERY,to_dataframe=True)

        new_data_master_shop_id['ingestion_time'] = pd.to_datetime(new_data_master_shop_id['ingestion_time']).dt.date
        old_data_master_shop_id['ingestion_time'] = pd.to_datetime(old_data_master_shop_id['ingestion_time']).dt.date

        combined_shop_ids = pd.concat([old_data_master_shop_id, new_data_master_shop_id])
        current_data_master_shop_id = combined_shop_ids.sort_values(by='ingestion_time').drop_duplicates(subset='shop_id', keep='last')
        current_data_master_shop_id['ingestion_time'] = pd.to_datetime(current_data_master_shop_id['ingestion_time'], utc=True)


        target_table = "cessa_price_monitoring.master_shop_id"
        staging_table = "cessa_price_monitoring.master_shop_id_staging"
        upsert_to_bq(current_data_master_shop_id, target_table, staging_table)


        current_data_master = current_data_master_shop_id.shop_id.to_list()



        return current_data_master





    @task(trigger_rule=TriggerRule.ALL_DONE)
    def extract_all_shop_products(current_data_master):
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import requests
        import time
        def fetch_data(base_url, params=None, proxies=None, max_retries=5, backoff_factor=3):
            for attempt in range(max_retries):
                try:
                    response = requests.get(base_url, params=params, proxies=proxies, verify=False)
                    response.raise_for_status()  # Raise HTTPError for bad responses
                    data = response.json()
                    return data
                except requests.exceptions.RequestException as e:
                    print(f"Attempt {attempt + 1} failed for params {params}: {e}")
                    if attempt < max_retries - 1:
                        sleep_time = backoff_factor * (2 ** attempt)
                        print(f"Retrying params {params} in {sleep_time} seconds...")
                        time.sleep(sleep_time)
                    else:
                        print(f"Max retries reached for params {params}. Failed to retrieve data.")
                        return None

        def fetch_all_data(base_url, param_list=None, proxies=None, max_retries=5, backoff_factor=3, max_workers=10):
            results = []

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                if param_list:
                    future_to_params = {
                        executor.submit(fetch_data, base_url, params, proxies, max_retries, backoff_factor): params for params in param_list
                    }
                else:
                    future_to_params = {
                        executor.submit(fetch_data, base_url, None, proxies, max_retries, backoff_factor): None
                    }

                for future in as_completed(future_to_params):
                    params = future_to_params[future]
                    try:
                        data = future.result()
                        if data:
                            results.append(data)
                    except Exception as exc:
                        print(f"Params {params} generated an exception: {exc}")

            return results
        


        base_url = "https://shopee.co.id/api/v4/search/search_items"
        params_list = []

        for row in current_data_master:
            result_dict = {
                'match_id': int(row),
                'page_type':'shop',
                'scenario' : 'PAGE_SHOP_SEARCH',
                'by': 'relevancy',
                'entry_point': 'ShopByPDP',
                'newest': 0,
                'order': 'desc',
                'view_session_id': '4e6aedf9-ca80-4ffd-8fa7-64dfe709ff89',
                'version': 2,
                'limit': 60,
                'keyword' : 'cessa'
                
            }
        params_list.append(result_dict)


        results = fetch_all_data(base_url=base_url, param_list=params_list,proxies=proxies)
        return results



    @task
    def transform_scrapping_data(results):
        data_results = transform_raw_data(results)
        data_results = data_results['product_list']
        df = pd.DataFrame(data_results)
        df['violated_price'] = df['sales_price'].apply(lambda x: True if x < 30000 else False)
        df_final = df[['product_name','sales_price','shop_url','product_url','violated_price','ingestion_time','total_product_sold']]
        df_final = df_final[df_final['total_product_sold'] > 0]
        df_final = df_final.to_dict(orient='records')
        df_final = df[['product_name','sales_price','shop_url','product_url','violated_price','ingestion_time','total_product_sold']]

        df_final = df_final.to_dict(orient='records')

        return df_final



    @task
    def load_scrapping_data(df_final):
        write_json_to_bq(df_final,dataset_id='cessa_price_monitoring',table_id='products')
        return "DONE"
    

    extract_task = extract_current_shop_products()
    extract_all_task = extract_all_shop_products(extract_task)
    transform_task = transform_scrapping_data(extract_all_task)
    load_scrapping_data(transform_task)




dag = cessa_price_monitoring()