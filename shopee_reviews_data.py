import os
import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.models.variable import Variable

local_tz = pendulum.timezone("Asia/Jakarta")
dataset_id = 'shopee_raw_data'
reviews_table = 'reviews'
order_list_table = 'order_status_streaming'
dag_id = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
schedule = Variable.get(
    f'schedule_{dag_id}', 
    default_var={"start_date":"2022-06-17","schedule_interval": None},
    deserialize_json=True
)



@dag(
    dag_id,
    start_date = pendulum.parse(schedule['start_date'], tz=local_tz),
    schedule_interval = schedule['schedule_interval'],
    catchup = False,
    default_args = {
        "depends_on_past": False,
        "owner" : 'Akbar',
        "retries": 6,
        "retry_delay": timedelta(minutes=8),
        "email": ['data-team@hypefast.id'],
        "email_on_failure": True
    },
    params={
        "date_from":"2022-10-01 01:00:00",
        "date_to":"2022-10-01 01:00:00",
        "force_all":True
    }
)



def shopee_reviews():



    @task
    def extract_load_order_data(**kwargs):
        import pandas as pd
        from datetime import datetime

        from common.helper_function import bq_query, write_json_to_bq
        from common.queries import SHOPEE_GET_ITEMS_SKU
        from common.logging_function import get_end_date_auto, update_success_date_all_brands, get_start_end_date, insert_job_log, update_success_date
        from modules.MarketplaceAPI import ShopeeAPI
        from common.connection import PostgreSQL
        import concurrent.futures


        def filter_by_dates(data, start_date, end_date):
            # Convert dates to datetime objects if they are strings
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d')
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, '%Y-%m-%d')
            
            def is_date_in_range(date_str, start, end):
                date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                return start <= date_obj <= end
            
            return [
                comment for comment in data
                if is_date_in_range(comment['create_time'], start_date, end_date)
            ]

        
        
        def fetch_and_process_comments(item_id,shopee_api,brand_access):
            # Fetch comments for a single item
            data_comment_single_comment = shopee_api.get_comment(brand_access, item_id=item_id)
            data_list = data_comment_single_comment[0]['data']
            brand_name = data_comment_single_comment[0]['brand_name']
            
            # Process and return the updated data
            results = []
            for datas in data_list:
                datas.update({'brand_name': brand_name})
                results.append(datas)
            return results
        def process_data_entry(data_entry):
            # Filter out entries with empty comments
            if not data_entry.get('comment', '').strip():
                return None

            # Convert ctime to datetime object and format it
            ctime = data_entry.get('create_time')
            if ctime:
                formatted_time = datetime.fromtimestamp(int(ctime)).strftime('%Y-%m-%d %H:%M')
                data_entry['create_time'] = formatted_time
            return data_entry


        shopee_api = ShopeeAPI(
            partner_id = Variable.get('shopee_partner_id'),
            partner_key = Variable.get('shopee_partner_secret')
        )

        # Set DAG name for manual trigger or auto
        if kwargs["dag_run"].conf.get('date_from') is not None:
            dag_suffix = '_manual'
        else:
            dag_suffix = ''


        dag_name = f'{dag_id}{dag_suffix}'
        end_date_auto = get_end_date_auto()
        if kwargs["dag_run"].conf.get('time_delta') is not None:
            val = int(kwargs["dag_run"].conf.get('time_delta'))
            delta = timedelta(days=val)
        else:
            delta = timedelta(days=1)


        update_success_date_all_brands(kwargs, dag_name)



        print("DAG name: ", dag_name)

        df = bq_query(SHOPEE_GET_ITEMS_SKU, to_dataframe=True)


        db_conn = PostgreSQL()
        master_brand_query = """

        SELECT master_brand_id,brand_name FROM data_master.master_brand
        where is_active = true
        ORDER BY master_brand_id ASC

        """

        master_brand_data = db_conn.execute(master_brand_query,return_df=True)
        df = pd.merge(df, master_brand_data, on='brand_name', how='inner')

        brand_list = df[['master_brand_id', 'brand_name']].drop_duplicates()
        # Converting the distinct rows into a list of tuples
        brand_list = list(brand_list.itertuples(index=False, name=None))

        shopee_brand_accesses = shopee_api.get_all_brand_access()
        find_by_brand_id = lambda brand_id: next((item for item in shopee_brand_accesses if item['brand_id'] == brand_id), None)


        for brand in brand_list:
            all_results = []
            brand_id = brand[0]
            brand_name = brand[1]

            start_date, end_date = get_start_end_date(kwargs, dag_name, brand_id, end_date_auto)
            print(brand_name, 'start_date', start_date, 'end_date', end_date)
            delta = timedelta(days=1)
            iter_date = start_date
            while (start_date.date() < end_date.date()):
                error_str = ''
                iter_date += delta
                if iter_date.date() == end_date.date():
                    iter_date = end_date
                print('iter_date', start_date, iter_date)

                df_products_brand = df[df['brand_name'] == brand_name]
                list_item = df_products_brand.item_id.unique()
                brand_access = find_by_brand_id(brand_id)




                try:

                    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                        # Map each item ID to the fetch_and_process_comments function
                        futures = [executor.submit(fetch_and_process_comments, item_id, shopee_api, brand_access) for item_id in list_item]
                        
                        # Consolidate results from all futures
                        results_list = []
                        for future in concurrent.futures.as_completed(futures):
                            results_list.extend(future.result())


                        for entry in results_list:
                            processed_entry = process_data_entry(entry)
                            if processed_entry:
                                wrapper_data = {
                                                'partition_by': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                                                'ingestion_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}
                                
                                processed_entry.update(wrapper_data)
                                all_results.append(processed_entry)

                    all_results = filter_by_dates(start_date,iter_date)

                except Exception as e:
                    error_str = f"""{str(e)}"""
                



                if len(error_str) == 0:
                    if len(all_results) > 0 :
                        write_json_to_bq(df, dataset_id, reviews_table) 
                        insert_job_log(dag_name, brand_id, len(all_results), str(start_date), "SUCCESS")
                        update_success_date(str(iter_date), dag_name, brand_id)
                    else :
                        pass

                else:
                    print('ERROR', error_str)
                    insert_job_log(dag_name, brand.master_brand_id, len(all_results), str(start_date), error_str)
                    raise Exception(error_str)
                

                start_date += delta




                









        





    extract_load_order_data()



dag = shopee_reviews()