import os
import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.models.variable import Variable

local_tz = pendulum.timezone("Asia/Jakarta")
dataset_id = 'shopee_raw_data'
escrow_table = 'escrow_details'
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
        "retries": 0,
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

def shopee_escrow_details():

    @task
    def extract_load_order_data(**kwargs):

        import traceback
        import pandas as pd
        import numpy as np
        from time import sleep

        from common.queries import SQL_QUERY_GET_BRAND_MASTER_DATA_ALL, SHOPEE_ORDER_DETAIL_GET_ORDERSN_ALL_BRANDS, SHOPEE_ESCROW_GET_ORDERSN
        from common.helper_function import bq_query, write_json_to_bq
        from common.logging_function import get_end_date_auto, update_success_date_all_brands, get_start_end_date, insert_job_log, update_success_date
        from modules.MarketplaceAPI import ShopeeAPI

        shopee_api = ShopeeAPI(
            partner_id = Variable.get('shopee_partner_id'),
            partner_key = Variable.get('shopee_partner_secret')
        )

        # get utcnow()
        end_date_auto = get_end_date_auto()
        query = SQL_QUERY_GET_BRAND_MASTER_DATA_ALL
        brand_df = bq_query(query, True)

        # get all order list based on manual input
        all_order_list = pd.DataFrame()
        dag_name = dag_id
        dag_suffix = ''
        try:
            query = SHOPEE_ORDER_DETAIL_GET_ORDERSN_ALL_BRANDS.format(str(kwargs["dag_run"].conf.get('date_from')), str(kwargs["dag_run"].conf.get('date_to')))
            all_order_list = bq_query(query, True)
            dag_suffix = '_manual'
            print('all_order_list length:', len(all_order_list))
        except:
            pass


        dag_name = f'{dag_name}{dag_suffix}'

        # update success date to start_date_manual if force_all = True
        update_success_date_all_brands(kwargs, dag_name)
        error_list = []
        for brand in brand_df.itertuples():
            try: # fault tolerance purpose
                start_date, end_date = get_start_end_date(kwargs, dag_name, brand.master_brand_id, end_date_auto)
                print(brand.brand_name, 'start_date', start_date, 'end_date', end_date)
                delta = timedelta(days=1)
                iter_date = start_date
                # iterate over range of dates
                while (start_date.date() < end_date.date()):
                    iter_date += delta
                    if iter_date.date() == end_date.date():
                        iter_date = end_date
                    print('iter_date', start_date, iter_date)

                    if len(all_order_list) == 0:
                        query = SHOPEE_ESCROW_GET_ORDERSN.format(f'{dataset_id}.{order_list_table}', str(start_date), str(iter_date), brand.shopee_id)
                        data_df = bq_query(query, True)
                    else:
                        data_df = all_order_list[(all_order_list['partition_by'] >= str(start_date)) & (all_order_list['partition_by'] <= str(iter_date)) & (all_order_list['shop_id'] == int(brand.shopee_id))]


                    brand_data = {}
                    data_list = list(set(data_df['ordersn'].to_list()))
                    brand_data[brand.brand_name] = data_list
                    print('data_list length:', len(data_list))

                    #brand_access = shopee_api.get_brand_access(brand.master_brand_id)
                    


                    error_str = ''
                    escrow_detail = []
                    

                    df_order_time = data_df[['ordersn','partition_by']].rename(columns={'ordersn':'order_sn'})
                    df_order_time = df_order_time.sort_values('partition_by').drop_duplicates('order_sn',keep='last').reset_index(drop=True)
                    chunk_size = 1000
                    chunk_escrow_detail = []


                    if len(brand_data[brand.brand_name]) > 2000:
                        batches = [brand_data[brand.brand_name][i:i+chunk_size] for i in range(0, len(brand_data[brand.brand_name]), chunk_size)]
                        for index, batch in enumerate(batches):

                            print(f"We are at batch {index} of {brand.brand_name}")
                            max_attempt = 10
                            backoff = 9
                            inner_brand_data =  {}
                            inner_brand_data[brand.brand_name] = batch

                            for n in range(max_attempt):
                                try:
                                    brand_access = shopee_api.get_brand_access(brand.master_brand_id)
                                    escrow_detail = shopee_api.get_escrow_details(brand_access, inner_brand_data,timeout = 120)
                                    if len(escrow_detail[0]['data']) != len(batch):
                                        print(batch)
                                        error_str = "Error: The sum of escrows_details is not equal as the sum of order list"
                                        raise Exception(error_str)
                                    print(f"Successfully get the escrows data on batch {index} of {brand.brand_name}")
                                    chunk_escrow_detail.append(escrow_detail)
                                    break


                                except Exception as e:
                                    e = traceback.format_exc()
                                    if n == max_attempt - 1:
                                        error_str = f"""{str(e)}"""
                                        raise Exception(error_str)
                                    
                                    print(f"error occured with attempt {n} of {max_attempt} in batch {index}, let's try again for this brand :  {brand.brand_name} with {backoff**2} Seconds" )
                                    sleep(backoff**2)
                                    backoff += 2


                    else:
                        max_attempt = 7
                        backoff = 7
                        for n in range(max_attempt):
                            try:
                                brand_access = shopee_api.get_brand_access(brand.master_brand_id)
                                escrow_detail = shopee_api.get_escrow_details(brand_access, brand_data)
                                # sanity check
                                if len(escrow_detail[0]['data']) != len(data_list):
                                    print(data_list)
                                    error_str = "Error: The sum of escrows_details is not equal as the sum of order list"
                                    raise Exception(error_str)
                                break


                            except Exception as e:
                                e = traceback.format_exc()
                                if n == max_attempt - 1:
                                    error_str = f"""{str(e)}"""
                                    break
                                print(f"error occured with attempt {n} of {max_attempt}, let's try again for this brand :  {brand.brand_name} with {backoff**2} Seconds" )
                                sleep(backoff**2)
                                backoff += 2



                    if len(str(error_str)) == 0:
                        msg = "SUCCESS"
                        print(msg)
                        results = []
                        if len(chunk_escrow_detail) != 0:
                            for escrow_detail in chunk_escrow_detail:
                                for data in escrow_detail:
                                    for record in data['data']:
                                        tmp_dict = {}
                                        tmp_dict['brand_name'] = data['brand_name']
                                        tmp_dict['marketplace'] = data['marketplace']
                                        tmp_dict['region'] = data['region']
                                        tmp_dict['order_sn'] = record['order_sn']
                                        tmp_dict['buyer_user_name'] = record['buyer_user_name']
                                        tmp_dict['ingestion_time'] = data['timestamp']
                                        if len(record['return_order_sn_list']) == 0:
                                            record['return_order_sn_list'].append('')

                                        tmp_dict['return_order_sn_list'] = record['return_order_sn_list']
                                        tmp_dict.update(record['order_income'])


                                        if len(tmp_dict['seller_voucher_code']) == 0:
                                        # just add empty string at first element
                                            tmp_dict['seller_voucher_code'].append('')


                                        tmp_dict['escrow_amount'] = int(round(tmp_dict['escrow_amount']))
                                        tmp_dict['refund_amount_to_buyer'] = tmp_dict['seller_return_refund']

                                        results.append(tmp_dict)
                        else:
                            for data in escrow_detail:
                                for record in data['data']:
                                    tmp_dict = {}
                                    tmp_dict['brand_name'] = data['brand_name']
                                    tmp_dict['marketplace'] = data['marketplace']
                                    tmp_dict['region'] = data['region']
                                    tmp_dict['order_sn'] = record['order_sn']
                                    tmp_dict['buyer_user_name'] = record['buyer_user_name']
                                    tmp_dict['ingestion_time'] = data['timestamp']

                                    if len(record['return_order_sn_list']) == 0:
                                        # just add empty string at first element
                                        record['return_order_sn_list'].append('')



                                    tmp_dict['return_order_sn_list'] = record['return_order_sn_list']
                                    tmp_dict.update(record['order_income'])



                                    if len(tmp_dict['seller_voucher_code']) == 0:
                                        # just add empty string at first element
                                        tmp_dict['seller_voucher_code'].append('')


                                    tmp_dict['escrow_amount'] = int(round(tmp_dict['escrow_amount']))
                                    tmp_dict['refund_amount_to_buyer'] = tmp_dict['seller_return_refund']


                                    
                                    results.append(tmp_dict)


                        if len(results) > 0:
                            df_results = pd.DataFrame(results)
                            #df = pd.merge(df_results, data_df, left_on='order_sn', right_on='ordersn')
                            df = df_results.merge(df_order_time, on='order_sn', how='inner')
                            df['partition_by'] = df.partition_by.apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
                            df.reset_index(drop=True, inplace=True)
                            df = df.replace({np.nan: 0})
                            # for i in df.itertuples():
                            #     print(iescrow_amount_after_adjustment)
                            float_to_int_list = ['escrow_amount_after_adjustment', 'total_adjustment_amount']
                            for i in float_to_int_list:
                                if i in df:
                                    df[i] = df[i].astype(int)
                            df = df.to_dict('records')

                            for data in df:
                                # Save order_adjustment as JSON
                                if 'order_adjustment' in data:
                                    if str(data['order_adjustment']):
                                        data['order_adjustment'] = None
                            
                            try:
                                write_json_to_bq(df, dataset_id, escrow_table) 
                            except Exception as e:
                                e = traceback.format_exc()
                                print(e)
                                # print(df)
                                raise Exception(e)

                            insert_job_log(dag_name, brand.master_brand_id, len(escrow_detail), str(start_date), msg)
                            update_success_date(str(iter_date), dag_name, brand.master_brand_id)          


                    else:
                        print('error', error_str)
                        insert_job_log(dag_name, brand.master_brand_id, len(escrow_detail), str(start_date), error_str)
                        if len(data_df) == 0:
                            pass
                        else:
                            raise Exception(error_str)

                    start_date += delta

            except Exception as e:
                error_list.append(f"***{str(brand.brand_name).upper()}*** Error Traceback: {str(e)}")
        
        if len(error_list) > 0:
            raise Exception(error_list)


    extract_load_order_data()

dag = shopee_escrow_details()