import json
import pandas as pd
import numpy as np
import requests
from config import CONF
from functools import reduce

class Analysis:
    def __init__(self, proposal_id, user_id, cate1, cate2, month):
        self.proposal_id = proposal_id
        self.user_id = user_id
        self.cate1 = cate1
        self.cate2 = cate2
        self.month = month
        self.shopping_item_file = f'output/shopping_item_{proposal_id}.csv'
        self.shopping_order_file = f'output/shopping_order_{proposal_id}.csv'

        api_key = CONF['MYD_API_KEY']
        self.base_url = f"https://openapi.insfiler.com/{api_key}/myd/shopping"

    def run(self):
        # Run analysis

        # J 분석 코드 추가 22.11.29
        # STEP 0. Read files & get variables
        shopping_item = pd.read_csv(self.shopping_item_file, dtype={'year':'object', 'month':'object'}, keep_default_na=False)
        shopping_order = pd.read_csv(self.shopping_order_file, dtype={'year':'object', 'month':'object'}, keep_default_na=False)
        shopping_var = self.select_var()

        # shopping_var 테이블에서 공통으로 쓰이는 단위 불러오기
        unit_person = \
        shopping_var[(shopping_var['model'] == 'common') & (shopping_var['key'] == 'person')]['value'].tolist()[0]
        unit_money_kr = \
        shopping_var[(shopping_var['model'] == 'common') & (shopping_var['key'] == 'money_kr')]['value'].tolist()[0]
        unit_item = \
        shopping_var[(shopping_var['model'] == 'common') & (shopping_var['key'] == 'item')]['value'].tolist()[0]
        unit_percent = \
        shopping_var[(shopping_var['model'] == 'common') & (shopping_var['key'] == 'percent')]['value'].tolist()[0]

        # STEP 1. ANALYZE_SECTION3
        # ITEM_SELECT CATE0, CATE1, CATE2
        item_cate0 = shopping_item.loc[shopping_item['month'] == self.month].copy()
        item_cate0 = item_cate0[['year', 'month', 'id', 'name_market', 'cost', 'name', 'category1']]

        item_cate1 = shopping_item.loc[(shopping_item['month'] == self.month) & (shopping_item['category1'] == self.cate1)].copy()
        item_cate1 = item_cate1[['year', 'month', 'id', 'name_market', 'cost', 'name', 'category1', 'category4']]

        item_cate2 = shopping_item.loc[(shopping_item['month'] == self.month) & (shopping_item['category1'] == self.cate1) & (
                    shopping_item['category2'] == self.cate2)].copy()
        item_cate2 = item_cate2[
            ['year', 'month', 'id', 'name_market', 'cost', 'name', 'category1', 'category2', 'category4']]

        # SECTION3
        shopping_section3 = pd.DataFrame()

        for i, item in enumerate([(item_cate0, '전체'), (item_cate1, self.cate1), (item_cate2, self.cate2)]):
            # SECTION3_CATE
            if i == 0:
                df_cate = item[0].groupby(['year', 'month', 'category1'])['cost'].agg(['sum', 'count']).reset_index()
                df_cate = df_cate.rename(columns={
                    'category1': 'word'
                })

                df_cate['cate'] = f'cate{i}_c1'
                df_cate['label'] = item[1]
            else:
                df_cate = item[0].groupby(['year', 'month', 'category4'])['cost'].agg(['sum', 'count']).reset_index()

                df_cate = df_cate.rename(columns={
                    'category4': 'word'
                })

                df_cate['cate'] = f'cate{i}_c4'
                df_cate['label'] = item[1]

            # SECTION3_MARKET
            df_market = item[0].groupby(['year', 'month', 'name_market'])['cost'].agg(['sum', 'count']).reset_index()

            df_market = df_market.rename(columns={
                'name_market': 'word'
            })

            df_market['cate'] = f'cate{i}_market'
            df_market['label'] = item[1]

            # SECTION3_ALL
            df_all = item[0].groupby(['year', 'month'])['cost'].agg(['sum', 'count']).reset_index()

            df_all['cate'] = f'cate{i}_total'
            df_all['label'] = item[1]
            df_all['word'] = '전체'

            # CONCAT
            df_concat = pd.concat((df_cate, df_market, df_all), axis=0)

            # CALCULATION PERCENT
            df_concat['word_per_money'] = df_concat['word'] + '(' + round(
                df_concat['sum'] / df_concat['sum'].max() * 100, 1).astype(str) + unit_percent + ')'
            df_concat['word_per_item'] = df_concat['word'] + '(' + round(
                df_concat['count'] / df_concat['count'].max() * 100, 1).astype(str) + unit_percent + ')'

            shopping_section3 = pd.concat((shopping_section3, df_concat), axis=0)

        # 불필요한 테이블 삭제
        del df_cate, df_market, df_all, df_concat

        # 테이블 정리
        shopping_section3 = shopping_section3.reset_index(drop=True)
        shopping_section3 = shopping_section3.rename(columns={
            'sum': 'money',
            'count': 'item'
        })

        shopping_section3['money'] = shopping_section3['money'].apply(lambda x: '{:,}'.format(int(x)) + unit_money_kr)
        shopping_section3['item'] = shopping_section3['item'].apply(lambda x: '{:,}'.format(int(x)) + unit_item)

        # 컬럼 정리
        shopping_section3 = shopping_section3[
            ['year', 'month', 'cate', 'label', 'word', 'money', 'word_per_money', 'item', 'word_per_item']]

        # STEP 2. ANALYZE_SECTION1
        # ORDER_SELECT CATE00
        order_cate0 = shopping_order.loc[(shopping_order['month'] == self.month) & (shopping_order['name_market'] == '월합계'),
                      :].copy()
        order_cate0 = order_cate0[['year', 'month', 'name_market', 'id', 'o_payment', 'cnt_item']]

        order_cate0['cate'] = 'cate0'
        order_cate0['label'] = '전체'

        order_cate0 = order_cate0.rename(columns={
            'o_payment': 'money',
            'cnt_item': 'item',
        })

        # ITEM_SELECT CATE1
        item_cate1['cate'] = 'cate1'
        item_cate1['name'] = 1

        item_cate1 = item_cate1.groupby(['year', 'month', 'cate', 'category1', 'id'])[
            ['cost', 'name']].sum().reset_index()

        item_cate1 = item_cate1.rename(columns={
            'category1': 'label',
            'cost': 'money',
            'name': 'item'
        })

        # ITEM_SELECT CATE2
        item_cate2['cate'] = 'cate2'
        item_cate2['name'] = 1

        item_cate2 = item_cate2.groupby(['year', 'month', 'cate', 'category2', 'id'])[
            ['cost', 'name']].sum().reset_index()

        item_cate2 = item_cate2.rename(columns={
            'category2': 'label',
            'cost': 'money',
            'name': 'item'
        })

        # SECTION1
        shopping_section1 = pd.concat((
            order_cate0[['year', 'month', 'cate', 'label', 'id', 'money', 'item']],
            item_cate1[['year', 'month', 'cate', 'label', 'id', 'money', 'item']],
            item_cate2[['year', 'month', 'cate', 'label', 'id', 'money', 'item']]
        ), axis=0)

        shopping_section1['id'] = 1

        shopping_section1 = shopping_section1.groupby(['year', 'month', 'cate', 'label'])[
            ['id', 'money', 'item']].sum().reset_index()

        # CALCULATION PERCENT
        shopping_section1['per_id'] = shopping_section1['id'] / shopping_section1['id'].shift(1) * 100
        shopping_section1['per_money'] = shopping_section1['money'] / shopping_section1['money'].shift(1) * 100
        shopping_section1['per_item'] = shopping_section1['item'] / shopping_section1['item'].shift(1) * 100

        # 단위 붙이기
        shopping_section1['id'] = shopping_section1['id'].apply(lambda x: '{:,}'.format(int(x)) + unit_person)
        shopping_section1['money'] = shopping_section1['money'].apply(lambda x: '{:,}'.format(int(x)) + unit_money_kr)
        shopping_section1['item'] = shopping_section1['item'].apply(lambda x: '{:,}'.format(int(x)) + unit_item)
        shopping_section1[['per_id', 'per_money', 'per_item']] = shopping_section1[
            ['per_id', 'per_money', 'per_item']].applymap(
            lambda x: str(int(round(x, 0))) + unit_percent if np.isnan(x) == False else x)

        # 컬럼 정리
        shopping_section1 = shopping_section1[
            ['year', 'month', 'cate', 'label', 'id', 'per_id', 'money', 'per_money', 'item', 'per_item']]

        # STEP 3. ANALYZE_SECTION2
        # SECTION2
        shopping_section2 = pd.concat((
            order_cate0[['year', 'month', 'cate', 'label', 'money', 'item']],
            item_cate1[['year', 'month', 'cate', 'label', 'money', 'item']],
            item_cate2[['year', 'month', 'cate', 'label', 'money', 'item']]
        ), axis=0)

        # groupby에 사용할 q1, q3 함수 정의
        q_25 = int(shopping_var[(shopping_var['model'] == 'shopping_section1') & (shopping_var['key'] == 'q1')][
                       'value'].values[0])
        q_75 = int(shopping_var[(shopping_var['model'] == 'shopping_section1') & (shopping_var['key'] == 'q3')][
                       'value'].values[0])

        def q1(x):
            return np.percentile(x, q_25)

        def q3(x):
            return np.percentile(x, q_75)

        shopping_section2 = shopping_section2.groupby(['year', 'month', 'cate', 'label'])[['money', 'item']].agg(
            ['count', 'sum', 'mean', 'min', q1, 'median', q3, 'max']).swaplevel(0, 1, axis=1).stack().reset_index()

        shopping_section2 = shopping_section2.rename(columns={
            'level_4': 'type',
            'count': 'cnt_id',
            'sum': 'total',
            'mean': 'avg'
        })

        shopping_section2['type'] = shopping_section2['type'].apply(lambda x: '소비금액' if x == 'money' else '소비물품')

        # 단위 붙이기
        shopping_section2['cnt_id'] = shopping_section2['cnt_id'].apply(lambda x: '{:,}'.format(int(x)) + unit_person)

        col_list = ['total', 'avg', 'min', 'q1', 'median', 'q3', 'max']
        shopping_section2[col_list] = shopping_section2[col_list].applymap(lambda x: '{:,}'.format(int(x)))
        shopping_section2[col_list] = shopping_section2.apply(
            lambda x: x[col_list] + unit_item if x['type'] == '소비물품' else x[col_list] + unit_money_kr, axis=1)

        # 컬럼 정리
        shopping_section2 = shopping_section2[
            ['year', 'month', 'cate', 'label', 'type', 'cnt_id', 'total', 'avg', 'min', 'q1', 'median', 'q3', 'max']]

        # 불필요한 테이블 정리
        del order_cate0, item_cate0, item_cate1, item_cate2

        return self.get_analysis_data(shopping_section1, shopping_section2, shopping_section3)

    def get_analysis_data(self, shopping_section1, shopping_section2, shopping_section3):
        '''
            return 결과 포맷 sample
            
            {
                "shopping_section1": [
                    {
                        "year": "2022",
                        "month": "07",
                        "cate": "cate0",
                        "label": "전체",
                        ...
                        "per_item": null
                    },{
                        "year": "2022",
                        "month": "07",
                        "cate": "cate1",
                        "label": "식품",
                        ...
                        "per_item": "44%"
                    }
                ],
                "shopping_section2": [
                    {
                        "year": "2022",
                        "month": "07",
                        ...
                        "q3": "176110원",
                        "max": "1589760원"
                    },{
                        "year": "2022",
                        "month": "07",
                        ...
                        "q3": "8개",
                        "max": "97개"
                    }
                ],
                "shopping_section3": [
                    {
                        "year": "2022",
                        "month": "07",
                        ...
                        "item": 9
                        "word_per_item": "가구/인테리어(1.0%)"
                    },{
                        "year": "2022",
                        "month": "07",
                        ...
                        "item": 2
                        "word_per_item": "도서(0.2%)"
                    }
                ]
            }
        '''

        return {
            'shopping_section1': json.loads(shopping_section1.to_json(orient='records')),
            'shopping_section2': json.loads(shopping_section2.to_json(orient='records')),
            'shopping_section3': json.loads(shopping_section3.to_json(orient='records')),
        }

    def select_var(self):
        url = f"{self.base_url}/var"

        payload = {
            'proposal_id': self.proposal_id,
            'user_id': self.user_id
        }
        res = requests.get(url, params=payload).json()

        return pd.DataFrame(res['result'])