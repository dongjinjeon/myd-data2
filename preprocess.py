import json
import pandas as pd
import numpy as np
import requests
from config import CONF
from functools import reduce
from Cryptodome.Cipher import AES

class Preprocess:
    def __init__(self, data_type, proposal_id, user_id, file_name, key, iv):
        self.data_type = data_type
        self.proposal_id = proposal_id
        self.user_id = user_id
        self.file_name = file_name
        self.key = key
        self.iv = iv

        api_key = CONF['MYD_API_KEY']
        self.base_url = f"https://openapi.insfiler.com/{api_key}/myd/shopping"

    def run(self):
        # Run preprocess

        json_data_arr = []
        total_shopping = []
        res = {}

        with open(self.file_name, 'rb') as file:
            origin = file.read()
            aes = AES.new(bytes.fromhex(self.key), AES.MODE_CBC, bytes.fromhex(self.iv))
            plain = aes.decrypt(origin)
            decoded = plain.decode('utf-8')
            # unpad
            decoded = decoded[:-ord(decoded[len(decoded) - 1:])]
            json_data = json.loads(decoded)

            json_data_arr.extend(json_data)

            for json_data in json_data_arr:

                id = json_data['id']
                create_at = json_data['createAt']

                data_types = json_data['payload']['data']

                for data_list in data_types:
                    if type(data_list) is list:
                        scheme = data_list[0]['identifier']['scheme']

                        raw_obj = {
                            'id': id,
                            'create_at': create_at,
                            'data_list': data_list
                        }

                        if scheme == 'OpenMarket':
                            total_shopping.append(raw_obj)

            if 'shopping' == self.data_type.lower():
                res = self.set_shopping_data(total_shopping)

        return res

    def set_shopping_data(self, total_shopping):

        total_data = []
        empty_order = 0
        empty_detail = 0

        for total in total_shopping:
            id = total['id']
            create_at = total['create_at']
            data_list = total['data_list']

            for data in data_list:
                name_market = data.get('name', None)
                orders = data.get('orders', None)

                row_data_obj = {
                    'id': id,
                    'create_at': create_at,
                    'name_market': name_market,
                }

                if orders is None or len(orders) == 0:
                    # order가 없는 market은 pass
                    # total_data.append(row_data_obj)
                    empty_order += 1
                    continue
                else:
                    for order in orders:
                        row_data_obj_order = row_data_obj.copy()
                        row_data_obj_order['date'] = order.get('date', None)
                        row_data_obj_order['amount'] = order.get('amount', None)
                        row_data_obj_order['order_number'] = order.get('orderNumber', None)
                        row_data_obj_order['shipping_cost'] = order.get('shippingCost', None)
                        row_data_obj_order['payment_amount'] = order.get('paymentAmount', None)
                        row_data_obj_order['saved_amount'] = order.get('savedAmount', None)

                        details = order.get('details', None)

                        if details is None or len(details) == 0:
                            # total_data.append(row_data_obj_order)
                            empty_detail += 1
                            continue
                        else:
                            for detail in details:
                                row_data_obj_detail = row_data_obj_order.copy()
                                row_data_obj_detail['amount_detail'] = detail.get('amount', None)
                                row_data_obj_detail['is_cancelled'] = detail.get('isCancelled', None)
                                row_data_obj_detail['count_detail'] = detail.get('count', None)
                                row_data_obj_detail['name_detail'] = detail.get('name', None)
                                row_data_obj_detail['unit_amount'] = detail.get('unitAmount', None)
                                row_data_obj_detail['status'] = detail.get('status', None)
                                row_data_obj_detail['option'] = detail.get('option', None)

                                categories = detail.get('categories', None)

                                # unset = '미분류'
                                unset = None

                                if categories is None or len(categories) == 0:
                                    row_data_obj_detail['category1'] = unset
                                    row_data_obj_detail['category2'] = unset
                                    row_data_obj_detail['category3'] = unset
                                    row_data_obj_detail['category4'] = unset
                                else:
                                    if len(categories) == 1:
                                        category1 = categories[0]['category']
                                        category2 = unset
                                        category3 = unset
                                        category4 = unset
                                    elif len(categories) == 2:
                                        category1 = categories[0]['category']
                                        category2 = categories[1]['category']
                                        category3 = unset
                                        category4 = unset
                                    elif len(categories) == 3:
                                        category1 = categories[0]['category']
                                        category2 = categories[1]['category']
                                        category3 = categories[2]['category']
                                        category4 = unset
                                    elif len(categories) == 4:
                                        category1 = categories[0]['category']
                                        category2 = categories[1]['category']
                                        category3 = categories[2]['category']
                                        category4 = categories[3]['category']
                                    else:
                                        raise Exception('categories error', id, order)

                                    row_data_obj_detail['category1'] = category1
                                    row_data_obj_detail['category2'] = category2
                                    row_data_obj_detail['category3'] = category3
                                    row_data_obj_detail['category4'] = category4

                                total_data.append(row_data_obj_detail)

        df = pd.DataFrame(total_data)

        # 전처리 1)
        # 한 id에 order가 2개 이상
        # 각 주문 내의 아이템 별로 group by 하여 중복되는 아이템 결제내역의 row number를 생성 (중복 중 category1이 null인 row를 버리기 위해)
        df['rn'] = df.sort_values(['id', 'name_market', 'date', 'order_number', 'name_detail', 'option', 'category1'],
                                  ascending=[True, True, True, True, True, True, True]) \
                       .groupby(['id', 'name_market', 'date', 'order_number', 'name_detail', 'option'], dropna=False) \
                       .cumcount() + 1

        # 중복 아이템의 row number가 2 이상이고 category1이 null인 row는 remove
        remove_filter_1 = (df.rn > 1) & (df.category1.isnull())
        removed = df.loc[remove_filter_1, :]

        df = df.loc[~remove_filter_1, :]

        # 전처리 2)
        # 한 order에 id가 2개 이상
        # 각 주문 별로 동일한 아이템을 create_at 기준으로 desc rank
        df["rk"] = df.groupby(['order_number', 'name_market', 'name_detail', 'option'], dropna=False)["create_at"] \
            .rank("dense", ascending=False)

        remove_filter_2 = (df.rk > 1)
        removed = df.loc[remove_filter_2, :]

        df = df.loc[~remove_filter_2, :]

        # 카테고리 None >> 미분류
        df['category1'] = df['category1'].fillna('미분류')
        df['category2'] = df['category2'].fillna('미분류')
        df['category3'] = df['category3'].fillna('미분류')
        df['category4'] = df['category4'].fillna('미분류')

        # NaN >> None
        df = df.replace({np.nan: None})

        # join with category_raw
        categories_df = self.get_shopping_categories(df)
        df_merged = pd.merge(left=df, right=categories_df, how='left', on=['category1', 'category2', 'category3', 'category4'])
        df_merged['category4'] = df_merged['category4_updated']
        df_merged.drop(columns=['category4_updated', 'category3_ch_cnt'])

        # keyword에 붙은 % 제거
        shopping_category = self.select_category_undefined()

        # 수치형컬럼 float로 형번환
        shopping = df_merged.copy()

        shopping = shopping.astype({
            'payment_amount': 'float',
            'shipping_cost': 'float',
            'amount': 'float',
            'saved_amount': 'float',
            'amount_detail': 'float',
            'count_detail': 'float',
            'unit_amount': 'float'
        })

        # null값 0 or 빈공간으로 채우기
        shopping['payment_amount'] = shopping['payment_amount'].fillna(0)
        shopping['shipping_cost'] = shopping['shipping_cost'].fillna(0)
        shopping['amount'] = shopping['amount'].fillna(0)
        shopping['saved_amount'] = shopping['saved_amount'].fillna(0)
        shopping['amount_detail'] = shopping['amount_detail'].fillna(0)
        shopping['count_detail'] = shopping['count_detail'].fillna(0)
        shopping['unit_amount'] = shopping['unit_amount'].fillna(0)
        shopping['option'] = shopping['option'].fillna('')

        # column명 수정
        shopping = shopping.rename(columns={
            'payment_amount': 'o_payment',
            'shipping_cost': 'o_shipping',
            'amount': 'o_amount',
            'saved_amount': 'o_saved',
            'amount_detail': 'item_amount',
            'count_detail': 'qty',
            'unit_amount': 'price',
            'name_detail': 'name'
        })

        # 취소, 반품, 환불, 품절의 status는 'cancel'로 변경
        shopping.loc[
            (shopping['status'].str.contains("취소") == True) | (shopping['status'].str.contains("반품") == True) | (
                shopping['status'].str.contains("환불")) | (shopping['status'].str.contains("품절")), 'status'] = 'cancel'

        # status_cancel 컬럼 추가
        shopping['status_cancel'] = shopping[shopping['status'] == 'cancel']['status']

        # STEP 2. count the number of items and without canceled items

        # In[ ]:

        # 주문별로 아이템개수의 합에서 취소된 아이템개수 빼기
        shopping_cnt_item = shopping[['id', 'date', 'order_number', 'name', 'status_cancel']].groupby(
            ['id', 'date', 'order_number']).count().add_prefix('cnt_').reset_index()
        shopping_cnt_item['cnt_item'] = shopping_cnt_item['cnt_name'] - shopping_cnt_item['cnt_status_cancel']

        # STEP 3. join cnt_item column

        # In[ ]:

        # 주문별로 구매 아이템개수 컬럼(cnt_item) 붙이기
        shopping = shopping.merge(
            shopping_cnt_item,
            on=['id', 'date', 'order_number'],
            how='left'
        )

        # 불필요한 테이블 삭제
        del shopping_cnt_item

        # 불필요한 컬럼 정리
        shopping = shopping[
            ['date', 'id', 'name_market', 'order_number', 'cnt_item', 'o_payment', 'o_shipping', 'o_amount', 'o_saved',
             'item_amount', 'qty', 'price', 'status', 'name', 'option', 'category1', 'category2', 'category3',
             'category4', 'create_at']]

        # STEP 4. filter out canceled orders (cnt_item is zero)

        # In[ ]:

        # 구매 아이템개수가 0(주문 전체가 취소된 경우)인 주문을 제외
        shopping = shopping[
            shopping['cnt_item'] != 0 & ~((shopping['o_payment'] == 0) & (shopping['status'] == 'cancel'))]

        # STEP 5. filter out pairs of canceled items

        # In[ ]:

        # 구매되었다가 취소된 아이템(status ok와 cancel을 모두 가진 아이템)을 제외
        # 중복제거 후 status 개수 세기
        shopping_ok_cancel = shopping[
            ['id', 'order_number', 'o_payment', 'name', 'option', 'status']].copy().drop_duplicates()
        shopping_ok_cancel = shopping_ok_cancel.groupby(['id', 'order_number', 'o_payment', 'name', 'option'])[
            ['status']].count().add_prefix('cnt_').reset_index()

        shopping = shopping.merge(
            shopping_ok_cancel,
            on=['id', 'order_number', 'o_payment', 'name', 'option'],
            how='left'
        )

        # 불필요한 테이블 삭제
        del shopping_ok_cancel

        # status_cnt가 1인 row만 남기기
        shopping = shopping[shopping['cnt_status'] == 1]

        shopping_market = self.select_category_market()

        # STEP 6. change market name + filter out individual canceled items and fill out the undefined categories
        shopping['name_market_modi'] = shopping['name_market'].map(dict(shopping_market.set_index('keyword')['name_market']))

        if shopping['name_market_modi'].isnull().sum() != 0:
            shopping['name_market_modi'] = shopping['name_market_modi'].fillna(shopping['name_market'])

        shopping['name_market'] = shopping['name_market_modi']
        shopping = shopping.drop(columns=['name_market_modi'])

        # 분류 테이블과 미분류 테이블로 분리
        shopping_defined = shopping[shopping['category1'] != '미분류'].copy()
        shopping_undefined = shopping[shopping['category1'] == '미분류'].copy()

        # 미분류 category의 상품명이 category 테이블의 keyword를 갖고 있으면 값을 바꾸는 함수
        def category_define(name, **args):
            for index, row in shopping_category.iterrows():
                if name.find(row['keyword']) != -1:
                    return pd.Series({'category1': row['category1'],
                                      'category2': row['category2'],
                                      'category3': row['category3'],
                                      'category4': row['category4']})
            return pd.Series({'category1': '미분류',
                              'category2': '미분류',
                              'category3': '미분류',
                              'category4': '미분류'})

        # 미분류 카테고리의 상품명 확인 후 category 컬럼 바꾸기
        shopping_undefined[['category1', 'category2', 'category3', 'category4']] = shopping_undefined.apply(
            lambda x: category_define(x['name']), axis=1)

        # 분류 테이블과 미분류 테이블 다시 합치기
        shopping = pd.concat((shopping_defined, shopping_undefined), axis=0)

        # 불필요한 테이블 삭제
        del shopping_defined, shopping_undefined

        # 남아있는 취소 아이템 제거 & 연도, 월 컬럼 추가
        shopping = shopping.loc[shopping['status'] != 'cancel', :]
        shopping['year'] = shopping['date'].str[0:4]
        shopping['month'] = shopping['date'].str[5:7]

        # STEP 7. create “item table”

        # In[ ]:

        # 아이템별 가격(단가 X 수량) 구하기
        shopping['cost'] = shopping['price'] * shopping['qty']

        # 불필요한 컬럼 제거
        shopping_item = shopping[
            ['year', 'month', 'id', 'name_market', 'order_number', 'cnt_item', 'o_payment', 'cost', 'price', 'qty',
             'name', 'option', 'category1', 'category2', 'category3', 'category4', 'create_at']].copy()

        # STEP 8-1. create “order table” : sum by order

        # In[ ]:

        # 주문별 수량합, 아이템 가격합
        shopping_order_1 = shopping.groupby(
            ['year', 'month', 'id', 'name_market', 'order_number', 'cnt_item', 'o_payment', 'o_shipping', 'o_saved',
             'create_at'])[['qty', 'cost', 'item_amount']].sum().add_prefix('sum_').reset_index()

        shopping_order_1['o_saved'] = shopping_order_1['sum_item_amount'] - shopping_order_1['sum_cost'] + \
                                      shopping_order_1['o_saved']
        shopping_order_1['cnt_order'] = np.NaN

        # 주문별 카테고리1 aggregation 구하기
        shopping_order_1_cate = shopping[['year', 'month', 'id', 'order_number', 'category1']].copy()
        shopping_order_1_cate['cnt'] = 1
        shopping_order_1_cate = shopping_order_1_cate.groupby(['year', 'month', 'id', 'order_number', 'category1'])[
            'cnt'].count().reset_index()
        shopping_order_1_cate['agg_c1'] = shopping_order_1_cate['category1'] + "(" + shopping_order_1_cate[
            'cnt'].astype(str) + ")"
        shopping_order_1_cate = shopping_order_1_cate.groupby(['year', 'month', 'id', 'order_number'])['agg_c1'].apply(
            lambda x: '; '.join(x)).reset_index()

        # 테이블 merge
        shopping_order_1 = shopping_order_1.merge(
            shopping_order_1_cate,
            on=['year', 'month', 'id', 'order_number'],
            how='left'
        )

        # 불필요한 테이블 삭제
        del shopping_order_1_cate

        shopping_order_1 = shopping_order_1.drop(columns=['sum_item_amount'])

        # STEP 8-2. create “order table” : sum by id/month

        # In[ ]:

        # 아이디별 월별 합 구하기
        shopping_order_2_cnt = shopping_order_1.groupby(['year', 'month', 'id'])[['order_number']].count().reset_index()
        shopping_order_2_sum = shopping_order_1.groupby(['year', 'month', 'id'])[
            ['cnt_item', 'sum_qty', 'sum_cost', 'o_saved', 'o_shipping', 'o_payment']].sum().reset_index()

        # 아이디별 월별 카테고리1 aggregation 구하기
        shopping_order_2_cate = shopping[['year', 'month', 'id', 'category1']].copy()
        shopping_order_2_cate['cnt'] = 1
        shopping_order_2_cate = shopping_order_2_cate.groupby(['year', 'month', 'id', 'category1'])[
            'cnt'].count().reset_index()
        shopping_order_2_cate['agg_c1'] = shopping_order_2_cate['category1'] + "(" + shopping_order_2_cate[
            'cnt'].astype(str) + ")"
        shopping_order_2_cate = shopping_order_2_cate.groupby(['year', 'month', 'id'])['agg_c1'].apply(
            lambda x: '; '.join(x)).reset_index()

        # 테이블 merge
        shopping_order_2 = reduce(lambda x, y: pd.merge(x, y, on=['year', 'month', 'id'], how='left'),
                                  [shopping_order_2_cnt, shopping_order_2_sum, shopping_order_2_cate])

        # 불필요한 테이블 삭제
        del shopping_order_2_cnt, shopping_order_2_sum, shopping_order_2_cate

        shopping_order_2 = shopping_order_2.rename(columns={
            'order_number': 'cnt_order',
        })

        shopping_order_2['name_market'] = '월합계'
        shopping_order_2['order_number'] = shopping_order_2['month'] + '월 합계'
        shopping_order_2['create_at'] = '월합계'

        # STEP 8-3. create “order table” : sum by id/total

        # In[ ]:

        # 아이디별 전 기간 총합 구하기
        shopping_order_3_cnt = shopping_order_1.groupby(['id'])[['order_number']].count().reset_index()
        shopping_order_3_sum = shopping_order_1.groupby(['id'])[
            ['cnt_item', 'sum_qty', 'sum_cost', 'o_saved', 'o_shipping', 'o_payment']].sum().reset_index()

        # 아이디별 월별 카테고리1 aggregation 구하기
        shopping_order_3_cate = shopping[['id', 'category1']].copy()
        shopping_order_3_cate['cnt'] = 1
        shopping_order_3_cate = shopping_order_3_cate.groupby(['id', 'category1'])['cnt'].count().reset_index()
        shopping_order_3_cate['agg_c1'] = shopping_order_3_cate['category1'] + "(" + shopping_order_3_cate[
            'cnt'].astype(str) + ")"
        shopping_order_3_cate = shopping_order_3_cate.groupby(['id'])['agg_c1'].apply(
            lambda x: '; '.join(x)).reset_index()

        # 테이블 merge
        shopping_order_3 = reduce(lambda x, y: pd.merge(x, y, on=['id'], how='left'),
                                  [shopping_order_3_cnt, shopping_order_3_sum, shopping_order_3_cate])

        # 불필요한 테이블 삭제
        del shopping_order_3_cnt, shopping_order_3_sum, shopping_order_3_cate

        shopping_order_3 = shopping_order_3.rename(columns={
            'order_number': 'cnt_order',
        })

        shopping_order_3['year'] = '0000'
        shopping_order_3['month'] = '00'
        shopping_order_3['name_market'] = '기간총계'
        shopping_order_3['order_number'] = '기간총계'
        shopping_order_3['create_at'] = '기간총계'

        # In[ ]:

        # 8-1, 8-2, 8-3 테이블 합친 후 인덱스 초기화
        shopping_order = pd.concat((shopping_order_1, shopping_order_2, shopping_order_3), axis=0)
        shopping_order = shopping_order.reset_index(drop=True)

        # 불필요한 테이블 삭제
        del shopping_order_1, shopping_order_2, shopping_order_3, shopping

        # 컬럼 정렬
        shopping_order = shopping_order[
            ['year', 'month', 'id', 'name_market', 'order_number', 'cnt_order', 'cnt_item', 'sum_qty', 'sum_cost',
             'o_saved', 'o_shipping', 'o_payment', 'agg_c1', 'create_at']]

        # # 이후 shopping_item, shopping_order 테이블 csv로 저장
        shopping_item.to_csv(f"output/shopping_item_{self.proposal_id}.csv", index=False)
        shopping_order.to_csv(f"output/shopping_order_{self.proposal_id}.csv", index=False)

        return self.get_preprocessed(shopping_item)

    def get_preprocessed(self, df):
        months = df.sort_values(['month'], ascending=[True])['month'].drop_duplicates().to_list()

        categories = {}
        cate_df = df[['category1', 'category2']].drop_duplicates().reset_index()
        for index, row in cate_df.iterrows():
            if row['category1'] not in categories:
                categories[row['category1']] = []
            categories[row['category1']].append(row['category2'])

        return {
            'months': months,
            'categories': categories,
        }

    def get_shopping_categories(self, df):
        categories_raw = df[['category1', 'category2', 'category3', 'category4']].drop_duplicates().sort_values(['category1', 'category2', 'category3', 'category4'], ascending=[True, True, True, True])
        categories_agg = categories_raw.groupby(['category1', 'category2', 'category3'])['category1'].count().reset_index(name='category3_ch_cnt')
        categories_merged = pd.merge(left=categories_raw, right=categories_agg, how='left', on=['category1', 'category2', 'category3'])

        conditions = [
            (categories_merged['category3'] != '미분류') & (categories_merged['category4'] == '미분류') & (categories_merged['category3_ch_cnt'] > 1),
            (categories_merged['category3'] != '미분류') & (categories_merged['category4'] == '미분류') & (categories_merged['category3_ch_cnt'] == 1),
        ]

        results = [
            categories_merged['category3'] + '(미분류)',
            categories_merged['category3']
        ]

        categories_merged['category4_updated'] = np.select(conditions, results, default=categories_merged['category4'])
        return categories_merged

    def select_category_undefined(self):
        url = f"{self.base_url}/category-undefined"

        payload = {
            'proposal_id': self.proposal_id,
            'user_id': self.user_id,
            'file_name': self.file_name
        }
        res = requests.get(url, params=payload).json()

        return pd.DataFrame(res['result'])

    def select_category_market(self):
        url = f"{self.base_url}/category-market"

        payload = {
            'proposal_id': self.proposal_id,
            'user_id': self.user_id,
            'file_name': self.file_name
        }
        res = requests.get(url, params=payload).json()

        return pd.DataFrame(res['result'])

