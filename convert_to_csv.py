from config import CONF
import pandas as pd
import numpy as np
from Cryptodome.Cipher import AES
import ujson
import concurrent.futures
import pickle
import csv
from pandas.api.types import is_numeric_dtype
import math
import sqlite3
from pathlib import Path
import traceback
from datetime import datetime
import gc

WORKING = "WORKING"
DONE = "DONE"
FAILED = "FAILED"

class ConvertToCSV:
    def __init__(self, file_names, key, iv, job_id, output_file_name, merge):
        self.job_id = job_id

        self.file_names = file_names
        self.current_file_name = self.file_names[0]
        self.output_file_name = output_file_name
        self.merge = merge

        self.key = key
        self.iv = iv
        self.SCHEME = {
            'USERINFO': 'UserInfo',
            'OPENMARKET': 'OpenMarket',
            'KEYWORD_ENGINE': 'KeywordEngine',
            'MEDICAL_RECORD': 'MedicalRecord',
            'MEDICAL_CHECKUP': 'MedicalCheckup',
            'FP_STOCK': 'FPStock',
            'FP_STOCK_ACCOUNT': 'FPStockAccount',
            'FP_BANK': 'FPBank',
            'FP_BANK_ACCOUNT': 'FPBankAccount',
            'FP_CARD': 'FPCard',
            'FP_CARD_ACCOUNT': 'FPCardAccount',
            'FP_INSURANCE': 'FPInsurance',
            'FP_INSURANCE_CONTRACT': 'FPInsuranceContract',
            'FP_INSURANCE_CAR_CONTRACT': 'FPInsuranceCarContract',
        }
        self.output_dir = 'output'

        # 초기화 obj
        self.default_obj = {
            'user_id': None,
            'data_created': None,

            # user field 초기화
            'user_gender': None,
            'user_birth': None,
            'user_region': None,
            'user_email': None,
            'user_installed_app': None,
            'user_device_model': None,
            'user_mobile_carrier': None,
            'user_name': None,
            'user_phone_number': None,
        }

        # 쇼핑 obj
        self.default_obj_shopping = {
            # order field 초기화
            'open_market_type': None,
            'open_market_name': None,
            'is_connected': None,
            'order_market_type': None,
            'order_number': None,
            'order_date': None,
            'order_payment_total': None,
            'order_shipping_cost': None,
            'order_saved_amount': None,
            'order_amount': None,
            'details_status': None,
            'details_is_cancelled': None,
            'details_unit_price': None,
            'details_unit_qty': None,
            'details_unit_amount': None,
            'details_unit_name': None,
            'details_unit_option': None,
            'category1': None,
            'category2': None,
            'category3': None,
            'category4': None,
        }

        # 검색 obj
        self.default_obj_search = {
            'platform_type': None,
            'platform_name': None,
            'is_connected': None,

            # keyword field 초기화
            'use_date': None,
            'keyword': None,
            'image_url': None,
            'engine_type': None,
            'service_type': None,
            'use_type': None,
        }

        # 진료/투약 obj
        self.default_obj_med_record = {
            # medicine field 초기화
            'patient_type': None,
            'treatment_type': None,
            'treatment_start_date': None,
            'hospital_name': None,
            'visit_days': None,
            'num_medications': None,
            'num_prescriptions': None,
            'medicine_treatment_type': None,
            'medicine_treatment_date': None,
            'medicine_medication_days': None,
            'medicine_effect': None,
            'medicine_name': None,
            'medicine_num_prescriptions': None,
        }

        # 검진일반 obj
        self.default_obj_med_checkup = {
            'data_category': None,
            'category_type': None,
            'checkup_date': None,
            'checkup_target': None,
            'target_type': None,
            'checkup_type': None,
            'organization': None,
            'checkup_place': None,
            'question_info': None,
            'opinion': None,
            'height': None,
            'weight': None,
            'waist': None,
            'bmi': None,
            'sight': None,
            'left_sight': None,
            'right_sight': None,
            'hearing': None,
            'left_hearing': None,
            'right_hearing': None,
            'blood_pressure': None,
            'high_blood_pressure': None,
            'low_blood_pressure': None,
            'total_cholesterol': None,
            'hdl_cholesterol': None,
            'ldl_cholesterol': None,
            'hemoglobin': None,
            'fasting_blood_glucose': None,
            'urinary_protein': None,
            'triglyceride': None,
            'serum_creatinine': None,
            'gfr': None,
            'ast': None,
            'alt': None,
            'ygpt': None,
            'tb_chest_disease': None,
            'osteoporosis': None,
            'judgement': None,
        }

        # 검진유아 obj
        self.default_obj_med_checkup_infant = {
            'data_category': None,
            'checkup_date': None,
            'checkup_target': None,
            'target_type': None,
            'checkup_type': None,
            'organization': None,
            'question_info': None,
            'opinion': None,
            'resident_id': None,
            'doctor_name': None,
            'license_number': None,
            'nursing_symbol': None,
            'document_title': None,

            # infantCheckup
            'issue_number': None,
            'issue_type': None,
            'issue_purpose': None,
            'sight_questionnaire': None,
            'sight_chart': None,
            'sight_test': None,
            'left_sight': None,
            'right_sight': None,
            'hearing_questionnaire': None,
            'development_evaluation_name': None,
            'development_evaluation_result': None,

            # infantDental
            'oral_health_awareness': None,
            'oral_problem_history': None,
            'oral_problematic_habit1': None,
            'oral_problematic_habit2': None,
            'oral_problematic_habit3': None,
            'oral_condition': None,
            'oral_restored_teeth': None,
            'oral_caries': None,
            'oral_risky_caries': None,
            'oral_proximal_caries': None,
            'oral_plague': None,
            'oral_hygiene': None,
            'oral_analysis': None,
            'oral_etc': None,

            'total_judgement': None,

            'result_type': None,
            'item': None,
            'result': None,
            'judgement': None,
            'reference': None,
            'remark': None,
        }

        # 증권 obj
        self.default_obj_stock = {
            # FPstock field 초기화
            'company_code': None,
            'company_name': None,
            'is_connected': None,
            'timestamp': None,

            # FPStockAccount field 초기화
            'account_company_code': None,
            'account_company_name': None,
            'account_number': None,
            'account_name': None,
            'account_type': None,
            'account_issue_date': None,
            'account_base_date': None,
            'account_is_tax_benefits': None,
            'account_search_date': None,
            'account_timestamp': None,

            # Basic, Asset 구분
            'data_category': None,

            # FPStockAccountBasic field 초기화
            'basics_deposit': None,
            'basics_currency': None,
            'basics_credit_loan_amount': None,
            'basics_mortgage_amount': None,
            'basics_timestamp': None,

            # FPStockAsset field 초기화
            'assets_product_type': None,
            'assets_product_type_detail': None,
            'assets_product_code': None,
            'assets_product_name': None,
            'assets_credit_type': None,
            'assets_quantity': None,
            'assets_purchase_amount': None,
            'assets_valuation_amount': None,
            'assets_currency': None,
            'assets_timestamp': None,
        }

        # 은행 obj
        self.default_obj_bank = {
            # FPBank field 초기화
            'company_code': None,
            'company_name': None,
            'is_connected': None,
            'timestamp': None,

            # FPBankAccount field 초기화
            'account_company_code': None,
            'account_company_name': None,
            'account_number': None,
            'account_name': None,
            'account_type': None,
            'account_status': None,
            'account_is_minus': None,
            'account_is_foreign_deposit': None,
            'account_is_trans_memo_agreed': None,
            'account_search_date': None,
            'account_timestamp': None,

            # Basic, Detail, Transaction 구분
            'data_category': None,

            # FPBankAccountBasic field 초기화
            'basics_saving_type': None,
            'basics_issue_date': None,
            'basics_expire_date': None,
            'basics_currency': None,
            'basics_timestamp': None,

            # FPBankAccountDetail field 초기화
            'details_balance': None,
            'details_withdraw_amount': None,
            'details_offered_rate': None,
            'details_latest_round': None,
            'details_currency': None,
            'details_timestamp': None,

            # FPBankTransaction field 초기화
            'transaction_time': None,
            'transaction_number': None,
            'transaction_type': None,
            'transaction_class': None,
            'transaction_amount': None,
            'transaction_balance': None,
            'transaction_memo': None,
            'transaction_currency': None,
        }

        # 카드 obj
        self.default_obj_card = {
            # FPCard field 초기화
            'company_code': None,
            'company_name': None,
            'is_connected': None,
            'timestamp': None,

            # FPCardAccount field 초기화
            'account_company_code': None,
            'account_company_name': None,
            'account_card_id': None,
            'account_card_number': None,
            'account_card_name': None,
            'account_card_type': None,
            'account_card_brand': None,
            'account_annual_fee': None,
            'account_issue_date': None,
            'account_is_traffic': None,
            'account_is_cashcard': None,
            'account_is_store_number_agreed': None,
            'account_search_date': None,
            'account_timestamp': None,

            # FPCardDomesticTransaction/FPCardForeignTransaction field 초기화
            'data_category': None,
            'transaction_approved_number': None,
            'transaction_approved_time': None,
            'transaction_approved_status': None,
            'transaction_payment_type': None,
            'transaction_store_name': None,
            'transaction_store_number': None,
            'transaction_approved_amount': None,
            'transaction_approved_amount_krw': None,
            'transaction_approved_country_code': None,
            'transaction_approved_currency': None,
            'transaction_modified_time': None,
            'transaction_modified_amount': None,
            'transaction_installment_count': None,
            'transaction_timestamp': None,
        }

        # 보험 obj
        self.default_obj_insurance = {
            # insurance field 초기화
            'company_code': None,
            'company_name': None,
            'is_connected': None,
            'timestamp': None,
            'data_category': None,

            # contract field 초기화
            'contract_company_code': None,
            'contract_company_name': None,
            'contract_policy_number': None,
            'contract_name': None,
            'contract_type': None,
            'contract_status': None,
            'contract_issue_date': None,
            'contract_expire_date': None,
            'contract_pay_amount': None,
            'contract_pay_cycle': None,
            'contract_pay_end_date': None,
            'contract_pay_count': None,
            'contract_krw_amount': None,
            'contract_currency': None,
            'contract_is_renewable': None,
            'contract_is_universal': None,
            'contract_is_variable': None,
            'contract_search_date': None,
            'contract_timestamp': None,

            # coverage field 초기화
            'coverage_name': None,
            'coverage_amount': None,
            'coverage_currency': None,
            'coverage_status': None,
            'coverage_expire_date': None,
            'coverage_timestamp': None,

            # car_contract field 초기화
            'car_name': None,
            'car_number': None,
            'car_contract_age': None,
            'car_contract_driver': None,
            'car_self_pay_amount': None,
            'car_self_pay_rate': None,
            'car_is_own_dmg_coverage': None,
        }

    def read_file(self, file):
        origin = file.read()
        aes = AES.new(bytes.fromhex(self.key), AES.MODE_CBC, bytes.fromhex(self.iv))
        plain = aes.decrypt(origin)
        decoded = plain.decode('utf-8')
        # unpad
        decoded = decoded[:-ord(decoded[len(decoded) - 1:])]
        return ujson.loads(decoded)
        # return ujson.load(file)                       # for test

    def read_json_size(self, file_name):
        with open(file_name, 'rb') as file:
            return len(self.read_file(file))

    def read_json_file(self, file_name, split_times, sep_size, idx):
        with open(file_name, 'rb') as file:
            start_idx = idx*sep_size
            end_idx = (idx + 1) * sep_size
            data = self.read_file(file)[start_idx:end_idx] if idx < split_times-1 else self.read_file(file)[start_idx:]
            for obj in data:
                yield obj

    def extract_json(self, json_data):
        # 전체 데이터를 담을 array
        raw_data_list = {
            'UNIDENTIFIED': [],
            self.SCHEME['OPENMARKET']: [],
            self.SCHEME['KEYWORD_ENGINE']: [],
            self.SCHEME['MEDICAL_RECORD']: [],
            self.SCHEME['MEDICAL_CHECKUP']: [],
            'MEDICAL_CHECKUP_INFANT': [],
            self.SCHEME['FP_STOCK']: [],
            self.SCHEME['FP_BANK']: [],
            self.SCHEME['FP_CARD']: [],
            self.SCHEME['FP_INSURANCE']: [],
        }

        row_obj = pickle.loads(pickle.dumps(self.default_obj))

        row_obj['user_id'] = json_data['id']
        row_obj['data_created'] = json_data['createAt']

        unidentified_data_obj = {
            'id': json_data['id'],
            'create_at': json_data['createAt'],
            "payload": {
                "data": []
            }
        }

        data_type_list = json_data['payload']['data']

        # 유저 정보 먼저 찾아오기
        for data_list in data_type_list:
            for data in data_list:
                if "identifier" in data:
                    identifier = data['identifier']
                    scheme = identifier['scheme']

                    if self.SCHEME['USERINFO'] == scheme:
                        row_obj['user_gender'] = data.get('gender', None)
                        row_obj['user_birth'] = data.get('birthDate', None)
                        row_obj['user_region'] = data.get('regionOfResidence', None)
                        row_obj['user_email'] = data.get('email', None)
                        row_obj['user_device_model'] = data.get('deviceModel', None)
                        row_obj['user_mobile_carrier'] = data.get('mobileNetworkOperator', None)
                        row_obj['user_name'] = data.get('name', None)
                        row_obj['user_phone_number'] = data.get('phoneNumber', None)

                        installed_app_list = data.get('installedAppList', None)
                        installed_app_name = None
                        if installed_app_list is not None:
                            installed_app_name = '/'.join(item['name'] for item in installed_app_list)

                        row_obj['user_installed_app'] = installed_app_name

                        # 현재 id에 data가 userinfo 밖에 없는 경우 unidentified에 빈 데이터로 id 기록
                        if len(data_type_list) == 1:
                            unidentified_data_obj['payload']['data'].append([])
                        break

        for data_list in data_type_list:
            unidentified_data = []
            for data in data_list:
                if "identifier" in data:
                    identifier = data['identifier']
                    scheme = identifier['scheme']

                    # OpenMarket scheme 탐색 시작
                    if self.SCHEME['OPENMARKET'] == scheme:
                        row_obj_data = pickle.loads(pickle.dumps(row_obj))
                        row_obj_shopping = pickle.loads(pickle.dumps(self.default_obj_shopping))
                        row_obj_data.update(row_obj_shopping)

                        row_obj_data['open_market_type'] = data.get('openMarketType', None)
                        row_obj_data['open_market_name'] = data.get('name', None)
                        row_obj_data['is_connected'] = data.get('isConnected', None)

                        if self.has_array('orders', data):
                            orders = data['orders']

                            for order in reversed(orders):
                                row_obj_order = pickle.loads(pickle.dumps(row_obj_data))
                                row_obj_order['order_market_type'] = order.get('openMarketType', None)
                                row_obj_order['order_number'] = order.get('orderNumber', None)
                                row_obj_order['order_date'] = order.get('date', None)
                                row_obj_order['order_payment_total'] = order.get('paymentAmount', None)
                                row_obj_order['order_shipping_cost'] = order.get('shippingCost', None)
                                row_obj_order['order_saved_amount'] = order.get('savedAmount', None)
                                row_obj_order['order_amount'] = order.get('amount', None)

                                if self.has_array('details', order):
                                    details = order['details']
                                    for detail in details:
                                        row_obj_detail = pickle.loads(pickle.dumps(row_obj_order))
                                        row_obj_detail['details_status'] = detail.get('status', None)
                                        row_obj_detail['details_is_cancelled'] = detail.get('isCancelled', None)
                                        row_obj_detail['details_unit_price'] = detail.get('unitAmount', None)
                                        row_obj_detail['details_unit_qty'] = detail.get('count', None)
                                        row_obj_detail['details_unit_amount'] = detail.get('amount', None)
                                        row_obj_detail['details_unit_name'] = detail.get('name', None)
                                        row_obj_detail['details_unit_option'] = detail.get('option', '')  # option이 None인 경우 빈값으로 처리 후 뒤에서 None으로 일괄 처리. 빈값으로 들어오는 case 한번에 처리하기 위해.
                                        row_obj_detail['category1'] = None
                                        row_obj_detail['category2'] = None
                                        row_obj_detail['category3'] = None
                                        row_obj_detail['category4'] = None

                                        if self.has_array('categories', detail):
                                            categories = detail['categories']

                                            # unset = '미분류'
                                            unset = None

                                            row_obj_detail['category1'] = unset
                                            row_obj_detail['category2'] = unset
                                            row_obj_detail['category3'] = unset
                                            row_obj_detail['category4'] = unset

                                            if categories is not None and len(categories) > 0:
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

                                                row_obj_detail['category1'] = category1
                                                row_obj_detail['category2'] = category2
                                                row_obj_detail['category3'] = category3
                                                row_obj_detail['category4'] = category4

                                        raw_data_list[self.SCHEME['OPENMARKET']].append(row_obj_detail)
                                        del row_obj_detail
                                else:
                                    raw_data_list[self.SCHEME['OPENMARKET']].append(row_obj_order)
                                    del row_obj_order
                        else:
                            # Order 정보 없는 케이스
                            raw_data_list[self.SCHEME['OPENMARKET']].append(row_obj_data)
                            del row_obj_data

                    # KeywordEngine 탐색 시작
                    elif self.SCHEME['KEYWORD_ENGINE'] == scheme:
                        row_obj_data = pickle.loads(pickle.dumps(row_obj))
                        row_obj_search = pickle.loads(pickle.dumps(self.default_obj_search))
                        row_obj_data.update(row_obj_search)

                        row_obj_data['platform_type'] = data.get('keywordType', None)
                        row_obj_data['platform_name'] = data.get('name', None)
                        row_obj_data['is_connected'] = data.get('isConnected', None)

                        if self.has_array('keywords', data):
                            keywords = data['keywords']
                            for keyword_idx, keyword in enumerate(keywords):
                                row_obj_keyword = row_obj_data.copy()
                                row_obj_keyword['use_date'] = keyword.get('dateTime', None)
                                row_obj_keyword['keyword'] = keyword.get('name', None)
                                row_obj_keyword['image_url'] = keyword.get('image', None)
                                row_obj_keyword['engine_type'] = keyword.get('keywordType', None)
                                row_obj_keyword['service_type'] = keyword.get('mainType', None)
                                row_obj_keyword['use_type'] = keyword.get('subType', None)

                                raw_data_list[self.SCHEME['KEYWORD_ENGINE']].append(row_obj_keyword)
                                del row_obj_keyword
                        else:
                            # Keyword 정보 없는 케이스
                            raw_data_list[self.SCHEME['KEYWORD_ENGINE']].append(row_obj_data)
                            del row_obj_data

                    elif self.SCHEME['MEDICAL_RECORD'] == scheme:
                        row_obj_data = pickle.loads(pickle.dumps(row_obj))
                        row_obj_med_record = pickle.loads(pickle.dumps(self.default_obj_med_record))
                        row_obj_data.update(row_obj_med_record)

                        row_obj_data['patient_type'] = data.get('type', None)
                        row_obj_data['treatment_type'] = data.get('treatmentType', None)
                        row_obj_data['treatment_start_date'] = data.get('treatmentStartDate', None)
                        row_obj_data['hospital_name'] = data.get('hospital', None)
                        row_obj_data['visit_days'] = data.get('visitDays', None)
                        row_obj_data['num_medications'] = data.get('numberOfMedications', None)
                        row_obj_data['num_prescriptions'] = data.get('numberOfPrescriptions', None)

                        if self.has_array('medicines', data):
                            medicines = data['medicines']
                            for medicine in medicines:
                                row_obj_medicine = pickle.loads(pickle.dumps(row_obj_data))
                                row_obj_medicine['medicine_treatment_type'] = medicine.get('treatmentType', None)
                                row_obj_medicine['medicine_treatment_date'] = medicine.get('treatmentDate', None)
                                row_obj_medicine['medicine_medication_days'] = medicine.get('medicationDays', None)
                                row_obj_medicine['medicine_effect'] = medicine.get('medicineEffect', None)
                                row_obj_medicine['medicine_name'] = medicine.get('medicineName', None)
                                row_obj_medicine['medicine_num_prescriptions'] = medicine.get('numberOfPrescriptions', None)

                                raw_data_list[self.SCHEME['MEDICAL_RECORD']].append(row_obj_medicine)
                                del row_obj_medicine
                        else:
                            # medicine정보 없는 케이스
                            raw_data_list[self.SCHEME['MEDICAL_RECORD']].append(row_obj_data)
                            del row_obj_data

                    elif self.SCHEME['MEDICAL_CHECKUP'] == scheme:
                        row_obj_data = pickle.loads(pickle.dumps(row_obj))
                        row_obj_checkup = pickle.loads(pickle.dumps(self.default_obj_med_checkup))
                        row_obj_checkup.update(row_obj_data)

                        row_obj_checkup['checkup_target'] = data.get('checkupTarget', None)

                        if self.has_array('references', data) or self.has_array('previews', data):
                            references = data.get('references', [])
                            previews = data.get('previews', [])

                            for reference in references:
                                row_obj_reference = pickle.loads(pickle.dumps(row_obj_checkup))

                                row_obj_reference['data_category'] = 'reference'
                                row_obj_reference['category_type'] = reference.get('type', None)
                                row_obj_reference['height'] = reference.get('height', None)
                                row_obj_reference['weight'] = reference.get('weight', None)
                                row_obj_reference['waist'] = reference.get('waist', None)
                                row_obj_reference['bmi'] = reference.get('bmi', None)
                                row_obj_reference['sight'] = reference.get('sight', None)
                                row_obj_reference['hearing'] = reference.get('hearing', None)
                                row_obj_reference['blood_pressure'] = reference.get('bloodPressure', None)
                                row_obj_reference['total_cholesterol'] = reference.get('totalCholesterol', None)
                                row_obj_reference['hdl_cholesterol'] = reference.get('hdlCholesterol', None)
                                row_obj_reference['ldl_cholesterol'] = reference.get('ldlCholesterol', None)
                                row_obj_reference['hemoglobin'] = reference.get('hemoglobin', None)
                                row_obj_reference['fasting_blood_glucose'] = reference.get('fastingBloodGlucose', None)
                                row_obj_reference['urinary_protein'] = reference.get('urinaryProtein', None)
                                row_obj_reference['triglyceride'] = reference.get('triglyceride', None)
                                row_obj_reference['serum_creatinine'] = reference.get('serumCreatinine', None)
                                row_obj_reference['gfr'] = reference.get('gfr', None)
                                row_obj_reference['ast'] = reference.get('ast', None)
                                row_obj_reference['alt'] = reference.get('alt', None)
                                row_obj_reference['ygpt'] = reference.get('ygpt', None)
                                row_obj_reference['tb_chest_disease'] = reference.get('tbChestDisease', None)
                                row_obj_reference['osteoporosis'] = reference.get('osteoporosis', None)

                                raw_data_list[self.SCHEME['MEDICAL_CHECKUP']].append(row_obj_reference)
                                del row_obj_reference

                            for preview in previews:
                                row_obj_preview = pickle.loads(pickle.dumps(row_obj_checkup))

                                row_obj_preview['data_category'] = 'preview'
                                row_obj_preview['checkup_date'] = preview.get('checkupDate', None)
                                row_obj_preview['checkup_place'] = preview.get('checkupPlace', None)
                                row_obj_preview['height'] = preview.get('height', None)
                                row_obj_preview['weight'] = preview.get('weight', None)
                                row_obj_preview['waist'] = preview.get('waist', None)
                                row_obj_preview['bmi'] = preview.get('bmi', None)
                                row_obj_preview['left_sight'] = preview.get('leftSight', None)
                                row_obj_preview['right_sight'] = preview.get('rightSight', None)
                                row_obj_preview['left_hearing'] = preview.get('leftHearing', None)
                                row_obj_preview['right_hearing'] = preview.get('rightHearing', None)
                                row_obj_preview['high_blood_pressure'] = preview.get('highBloodPressure', None)
                                row_obj_preview['low_blood_pressure'] = preview.get('lowBloodPressure', None)
                                row_obj_preview['total_cholesterol'] = preview.get('totalCholesterol', None)
                                row_obj_preview['hdl_cholesterol'] = preview.get('hdlCholesterol', None)
                                row_obj_preview['ldl_cholesterol'] = preview.get('ldlCholesterol', None)
                                row_obj_preview['hemoglobin'] = preview.get('hemoglobin', None)
                                row_obj_preview['fasting_blood_glucose'] = preview.get('fastingBloodGlucose', None)
                                row_obj_preview['urinary_protein'] = preview.get('urinaryProtein', None)
                                row_obj_preview['triglyceride'] = preview.get('triglyceride', None)
                                row_obj_preview['serum_creatinine'] = preview.get('serumCreatinine', None)
                                row_obj_preview['gfr'] = preview.get('gfr', None)
                                row_obj_preview['ast'] = preview.get('ast', None)
                                row_obj_preview['alt'] = preview.get('alt', None)
                                row_obj_preview['ygpt'] = preview.get('ygpt', None)
                                row_obj_preview['tb_chest_disease'] = preview.get('tbChestDisease', None)
                                row_obj_preview['osteoporosis'] = preview.get('osteoporosis', None)
                                row_obj_preview['judgement'] = preview.get('judgement', None)

                                raw_data_list[self.SCHEME['MEDICAL_CHECKUP']].append(row_obj_preview)
                                del row_obj_preview

                        if self.has_array('results', data):
                            results = data['results']

                            for result in results:
                                if result.get('type') == '0':
                                    row_obj_checkup_result = pickle.loads(pickle.dumps(row_obj_checkup))

                                    row_obj_checkup_result['data_category'] = 'result'
                                    row_obj_checkup_result['checkup_date'] = result.get('checkupDate', None)
                                    row_obj_checkup_result['target_type'] = result.get('type', None)
                                    row_obj_checkup_result['checkup_type'] = result.get('checkupType', None)
                                    row_obj_checkup_result['organization'] = result.get('organization', None)
                                    row_obj_checkup_result['question_info'] = result.get('questionInfo', None)
                                    row_obj_checkup_result['opinion'] = result.get('opinion', None)
                                    row_obj_checkup_result['original_data'] = result.get('originalData', None)

                                    raw_data_list[self.SCHEME['MEDICAL_CHECKUP']].append(row_obj_checkup_result)
                                    del row_obj_checkup_result

                                elif (result.get('type') == '1') or (result.get('type') == '2'):
                                    row_obj_data = pickle.loads(pickle.dumps(row_obj))
                                    row_obj_infant_checkup = pickle.loads(pickle.dumps(self.default_obj_med_checkup_infant))
                                    row_obj_infant_checkup.update(row_obj_data)

                                    row_obj_infant_checkup['checkup_date'] = result.get('checkupDate', None)
                                    row_obj_infant_checkup['target_type'] = result.get('type', None)
                                    row_obj_infant_checkup['checkup_type'] = result.get('checkupType', None)
                                    row_obj_infant_checkup['organization'] = result.get('organization', None)
                                    row_obj_infant_checkup['question_info'] = result.get('questionInfo', None)
                                    row_obj_infant_checkup['opinion'] = result.get('opinion', None)
                                    row_obj_infant_checkup['original_data'] = result.get('originalData', None)

                                    for identifier in ['infantMedicalCheckups', 'infantDentalCheckups']:
                                        infant_checkups = result.get(identifier, [])

                                        for infant_checkup in infant_checkups:
                                            row_obj_infant_result = pickle.loads(pickle.dumps(row_obj_infant_checkup))

                                            row_obj_infant_result['data_category'] = identifier
                                            row_obj_infant_result['checkup_target'] = infant_checkup.get('checkupTarget', None)
                                            row_obj_infant_result['resident_id'] = infant_checkup.get('residentID', None)
                                            row_obj_infant_result['doctor_name'] = infant_checkup.get('doctor', None)
                                            row_obj_infant_result['license_number'] = infant_checkup.get('licenseNumber', None)
                                            row_obj_infant_result['nursing_symbol'] = infant_checkup.get('nursingSymbol', None)
                                            row_obj_infant_result['document_title'] = infant_checkup.get('documentTitle', None)
                                            row_obj_infant_result['issue_number'] = infant_checkup.get('issueNumber', None)
                                            row_obj_infant_result['issue_type'] = infant_checkup.get('type', None)
                                            row_obj_infant_result['issue_purpose'] = infant_checkup.get('purpose', None)
                                            row_obj_infant_result['sight_questionnaire'] = infant_checkup.get('sightQuestionnaire', None)
                                            row_obj_infant_result['sight_chart'] = infant_checkup.get('sightChart', None)
                                            row_obj_infant_result['sight_test'] = infant_checkup.get('sightTest', None)
                                            row_obj_infant_result['left_sight'] = infant_checkup.get('leftSight', None)
                                            row_obj_infant_result['right_sight'] = infant_checkup.get('rightSight', None)
                                            row_obj_infant_result['hearing_questionnaire'] = infant_checkup.get('hearingQuestionnaire', None)
                                            row_obj_infant_result['development_evaluation_name'] = infant_checkup.get('developmentEvaluationName', None)
                                            row_obj_infant_result['development_evaluation_result'] = infant_checkup.get('developmentEvaluationResult', None)
                                            row_obj_infant_result['oral_health_awareness'] = infant_checkup.get('healthAwareness', None)
                                            row_obj_infant_result['oral_problem_history'] = infant_checkup.get('problemHistory', None)
                                            row_obj_infant_result['oral_problematic_habit1'] = infant_checkup.get('problematicHabit1', None)
                                            row_obj_infant_result['oral_problematic_habit2'] = infant_checkup.get('problematicHabit2', None)
                                            row_obj_infant_result['oral_problematic_habit3'] = infant_checkup.get('problematicHabit3', None)
                                            row_obj_infant_result['oral_condition'] = infant_checkup.get('condition', None)
                                            row_obj_infant_result['oral_restored_teeth'] = infant_checkup.get('restoredTeeth', None)
                                            row_obj_infant_result['oral_caries'] = infant_checkup.get('caries', None)
                                            row_obj_infant_result['oral_risky_caries'] = infant_checkup.get('riskyCaries', None)
                                            row_obj_infant_result['oral_proximal_caries'] = infant_checkup.get('proximalCaries', None)
                                            row_obj_infant_result['oral_plague'] = infant_checkup.get('plague', None)
                                            row_obj_infant_result['oral_hygiene'] = infant_checkup.get('hygieneTest', None)
                                            row_obj_infant_result['oral_analysis'] = infant_checkup.get('resultAnalysis', None)
                                            row_obj_infant_result['oral_etc'] = infant_checkup.get('etcOpinion', None)
                                            row_obj_infant_result['total_judgement'] = infant_checkup.get('totalJudgement', None)

                                            if self.has_array('physicalOpinions', infant_checkup) or self.has_array('physicalExaminations', infant_checkup) or self.has_array('healthEducations', infant_checkup) or self.has_array('guides', infant_checkup) or self.has_array('actions', infant_checkup):
                                                for subIdentifier in [('physicalOpinions', 'opinions'), ('physicalExaminations', 'examinations'), ('healthEducations', 'educations'), ('guides', 'guides'), ('actions', 'actions')]:
                                                    infant_sub_checkups = infant_checkup.get(subIdentifier[0], [])

                                                    for infant_sub_checkup in infant_sub_checkups:
                                                        row_obj_infant_sub_checkup = pickle.loads(pickle.dumps(row_obj_infant_result))

                                                        row_obj_infant_sub_checkup['result_type'] = subIdentifier[1]
                                                        row_obj_infant_sub_checkup['item'] = infant_sub_checkup.get('item', None)
                                                        row_obj_infant_sub_checkup['result'] = infant_sub_checkup.get('result', None)
                                                        row_obj_infant_sub_checkup['judgement'] = infant_sub_checkup.get('judgement', None)
                                                        row_obj_infant_sub_checkup['reference'] = infant_sub_checkup.get('reference', None)
                                                        row_obj_infant_sub_checkup['remark'] = infant_sub_checkup.get('remark', None)

                                                        raw_data_list['MEDICAL_CHECKUP_INFANT'].append(row_obj_infant_sub_checkup)
                                                        del row_obj_infant_sub_checkup
                                            else:
                                                raw_data_list['MEDICAL_CHECKUP_INFANT'].append(row_obj_infant_result)
                                                del row_obj_infant_result

                    elif self.SCHEME['FP_STOCK'] == scheme:
                        row_obj_data = pickle.loads(pickle.dumps(row_obj))
                        row_obj_stock = pickle.loads(pickle.dumps(self.default_obj_stock))
                        row_obj_data.update(row_obj_stock)

                        row_obj_data['company_code'] = data.get('companyCode', None)
                        row_obj_data['company_name'] = data.get('companyName', None)
                        row_obj_data['is_connected'] = data.get('isConnected', None)
                        row_obj_data['timestamp'] = data.get('timestamp', None)

                        if self.has_array('accounts', data):
                            accounts = data['accounts']
                            for account in accounts:
                                row_obj_account = pickle.loads(pickle.dumps(row_obj_data))
                                total_data_account = self.find_stock_account(row_obj_account, account)
                                raw_data_list[self.SCHEME['FP_STOCK']].extend(total_data_account)
                                del total_data_account
                        else:
                            # accounts 데이터가 없는 경우
                            raw_data_list[self.SCHEME['FP_STOCK']].append(row_obj_data)
                            del row_obj_data

                    elif self.SCHEME['FP_STOCK_ACCOUNT'] == scheme:
                        row_obj_data = pickle.loads(pickle.dumps(row_obj))
                        row_obj_stock = pickle.loads(pickle.dumps(self.default_obj_stock))
                        row_obj_data.update(row_obj_stock)

                        total_data_account = self.find_stock_account(row_obj_data, data)
                        raw_data_list[self.SCHEME['FP_STOCK']].extend(total_data_account)
                        del total_data_account

                    # FP_BANK
                    elif self.SCHEME['FP_BANK'] == scheme:
                        row_obj_data = pickle.loads(pickle.dumps(row_obj))
                        row_obj_bank = pickle.loads(pickle.dumps(self.default_obj_bank))
                        row_obj_data.update(row_obj_bank)

                        row_obj_data['company_code'] = data.get('companyCode', None)
                        row_obj_data['company_name'] = data.get('companyName', None)
                        row_obj_data['is_connected'] = data.get('isConnected', None)
                        row_obj_data['timestamp'] = data.get('timestamp', None)

                        if self.has_array('accounts', data):
                            accounts = data['accounts']
                            for account in accounts:
                                row_obj_account = pickle.loads(pickle.dumps(row_obj_data))
                                total_data_account = self.find_bank_account(row_obj_account, account)
                                raw_data_list[self.SCHEME['FP_BANK']].extend(total_data_account)
                                del total_data_account

                        else:
                            # accounts 데이터가 없는 경우
                            raw_data_list[self.SCHEME['FP_BANK']].append(row_obj_data)
                            del row_obj_data

                    elif self.SCHEME['FP_BANK_ACCOUNT'] == scheme:
                        row_obj_data = pickle.loads(pickle.dumps(row_obj))
                        row_obj_bank = pickle.loads(pickle.dumps(self.default_obj_bank))
                        row_obj_data.update(row_obj_bank)

                        total_data_account = self.find_bank_account(row_obj_data, data)
                        raw_data_list[self.SCHEME['FP_BANK']].extend(total_data_account)
                        del total_data_account

                    # FP_CARD
                    elif self.SCHEME['FP_CARD'] == scheme:
                        row_obj_data = pickle.loads(pickle.dumps(row_obj))
                        row_obj_card = pickle.loads(pickle.dumps(self.default_obj_card))
                        row_obj_data.update(row_obj_card)

                        row_obj_data['company_code'] = data.get('companyCode', None)
                        row_obj_data['company_name'] = data.get('companyName', None)
                        row_obj_data['is_connected'] = data.get('isConnected', None)
                        row_obj_data['timestamp'] = data.get('timestamp', None)

                        if self.has_array('accounts', data):
                            accounts = data['accounts']
                            for account in accounts:
                                row_obj_account = pickle.loads(pickle.dumps(row_obj_data))
                                total_data_account = self.find_card_account(row_obj_account, account)
                                raw_data_list[self.SCHEME['FP_CARD']].extend(total_data_account)
                                del total_data_account
                        else:
                            # accounts 데이터가 없는 경우
                            raw_data_list[self.SCHEME['FP_CARD']].append(row_obj_data)
                            del row_obj_data

                    elif self.SCHEME['FP_CARD_ACCOUNT'] == scheme:
                        row_obj_data = pickle.loads(pickle.dumps(row_obj))
                        row_obj_bank = pickle.loads(pickle.dumps(self.default_obj_card))
                        row_obj_data.update(row_obj_bank)

                        total_data_account = self.find_card_account(row_obj_data, data)
                        raw_data_list[self.SCHEME['FP_CARD']].extend(total_data_account)
                        del total_data_account

                    # FP_INSURANCE
                    elif self.SCHEME['FP_INSURANCE'] == scheme:
                        row_obj_data = pickle.loads(pickle.dumps(row_obj))
                        row_obj_insurance = pickle.loads(pickle.dumps(self.default_obj_insurance))
                        row_obj_data.update(row_obj_insurance)

                        row_obj_data['company_code'] = data.get('companyCode', None)
                        row_obj_data['company_name'] = data.get('companyName', None)
                        row_obj_data['is_connected'] = data.get('isConnected', None)
                        row_obj_data['timestamp'] = data.get('timestamp', None)

                        if self.has_array('contracts', data) or self.has_array('carContracts', data):
                            contracts = data.get('contracts', [])
                            carContracts = data.get('carContracts', [])

                            for contract in contracts:
                                row_obj_contract = pickle.loads(pickle.dumps(row_obj_data))
                                total_data_contract = self.find_insurance_contract(row_obj_contract, contract, self.SCHEME['FP_INSURANCE_CONTRACT'])
                                raw_data_list[self.SCHEME['FP_INSURANCE']].extend(total_data_contract)
                                del total_data_contract

                            for carContract in carContracts:
                                row_obj_contract = pickle.loads(pickle.dumps(row_obj_data))
                                total_data_contract = self.find_insurance_contract(row_obj_contract, carContract, self.SCHEME['FP_INSURANCE_CAR_CONTRACT'])
                                raw_data_list[self.SCHEME['FP_INSURANCE']].extend(total_data_contract)
                                del total_data_contract

                        else:
                            # contracts, carContracts 데이터가 모두 없는 경우
                            raw_data_list[self.SCHEME['FP_INSURANCE']].append(row_obj_data)
                            del row_obj_data

                    elif self.SCHEME['FP_INSURANCE_CONTRACT'] == scheme or self.SCHEME['FP_INSURANCE_CAR_CONTRACT'] == scheme:
                        row_obj_data = pickle.loads(pickle.dumps(row_obj))
                        row_obj_insurance = pickle.loads(pickle.dumps(self.default_obj_insurance))
                        row_obj_data.update(row_obj_insurance)

                        total_data_contract = self.find_insurance_contract(row_obj_data, data, scheme)
                        raw_data_list[self.SCHEME['FP_INSURANCE']].extend(total_data_contract)
                        del total_data_contract

                    elif self.SCHEME['USERINFO'] != scheme:
                        unidentified_data.append(data)
                else:
                    unidentified_data.append(data)

            if len(unidentified_data) > 0:
                unidentified_data_obj['payload']['data'].append(unidentified_data)
        del row_obj

        if len(unidentified_data_obj['payload']['data']) > 0:
            raw_data_list['UNIDENTIFIED'].append(unidentified_data_obj)
            del unidentified_data_obj

        return raw_data_list

    def update_status(self, status, fail_log):
       
        conn = sqlite3.connect(CONF['CSV_STATUS_DB'])
        c = conn.cursor()
        current_datetime = datetime.now()

        if status == WORKING:
            # 첫 insert
            c.execute("""INSERT INTO job_status (job_id, status, fail_log, create_datetime, update_datetime) 
                            VALUES (?, ?, ?, ?, ?);""", [self.job_id, status, '', current_datetime, current_datetime])
        else:
            # 완료/실패인 경우 상태 update
            c.execute("""UPDATE job_status 
                            SET status = ?
                            , fail_log = ?
                            , update_datetime = ? 
                            WHERE job_id = ?;""", [status, fail_log, current_datetime, self.job_id])
        conn.commit()
        c.close()
        conn.close()

    def start(self):
        try:
            for file_name in self.file_names:
                self.run(file_name)
            self.update_status(DONE, '')
        except Exception:
            self.update_status(FAILED, traceback.format_exc())
        finally:
            gc.collect()

    def run(self, file_name):
        self.current_file_name = file_name

        # json 사이즈 read
        json_size = self.read_json_size(file_name)

        # config 단위로 분할 개수 정해서 read 할때 전달
        sep_size = CONF['FILE_SEP_SIZE']
        split_times = math.ceil(json_size / sep_size)

        for idx in range(split_times):
            raw_data_list = {
                'UNIDENTIFIED': [],
                self.SCHEME['OPENMARKET']: [],
                self.SCHEME['KEYWORD_ENGINE']: [],
                self.SCHEME['MEDICAL_RECORD']: [],
                self.SCHEME['MEDICAL_CHECKUP']: [],
                'MEDICAL_CHECKUP_INFANT': [],
                self.SCHEME['FP_STOCK']: [],
                self.SCHEME['FP_BANK']: [],
                self.SCHEME['FP_CARD']: [],
                self.SCHEME['FP_INSURANCE']: [],
            }

            with concurrent.futures.ThreadPoolExecutor() as executor:
                json_file = self.read_json_file(file_name, split_times, sep_size, idx)
                future_to_data = {executor.submit(self.extract_json, json_data): json_data for json_data in json_file}
                for future in concurrent.futures.as_completed(future_to_data):
                    raw = future.result()
                    raw_data_list[self.SCHEME['OPENMARKET']] += raw[self.SCHEME['OPENMARKET']]
                    raw_data_list[self.SCHEME['KEYWORD_ENGINE']] += raw[self.SCHEME['KEYWORD_ENGINE']]
                    raw_data_list[self.SCHEME['MEDICAL_RECORD']] += raw[self.SCHEME['MEDICAL_RECORD']]
                    raw_data_list[self.SCHEME['MEDICAL_CHECKUP']] += raw[self.SCHEME['MEDICAL_CHECKUP']]
                    raw_data_list['MEDICAL_CHECKUP_INFANT'] += raw['MEDICAL_CHECKUP_INFANT']
                    raw_data_list[self.SCHEME['FP_STOCK']] += raw[self.SCHEME['FP_STOCK']]
                    raw_data_list[self.SCHEME['FP_BANK']] += raw[self.SCHEME['FP_BANK']]
                    raw_data_list[self.SCHEME['FP_CARD']] += raw[self.SCHEME['FP_CARD']]
                    raw_data_list[self.SCHEME['FP_INSURANCE']] += raw[self.SCHEME['FP_INSURANCE']]
                    raw_data_list['UNIDENTIFIED'] += raw['UNIDENTIFIED']

                    del raw
                del json_file

            self.preprocess(raw_data_list)

    def preprocess(self, raw_data_list):
        raw_data_shopping = raw_data_list[self.SCHEME['OPENMARKET']]
        if len(raw_data_shopping) > 0:
            self.preprocess_shopping(raw_data_shopping)

        raw_data_search = raw_data_list[self.SCHEME['KEYWORD_ENGINE']]
        if len(raw_data_search) > 0:
            self.preprocess_search(raw_data_search)

        raw_data_med_record = raw_data_list[self.SCHEME['MEDICAL_RECORD']]
        if len(raw_data_med_record) > 0:
            self.preprocess_med_record(raw_data_med_record)

        raw_data_med_checkup = raw_data_list[self.SCHEME['MEDICAL_CHECKUP']]
        if len(raw_data_med_checkup) > 0:
            self.preprocess_med_checkup(raw_data_med_checkup)

        raw_data_med_checkup_infant = raw_data_list['MEDICAL_CHECKUP_INFANT']
        if len(raw_data_med_checkup_infant) > 0:
            self.preprocess_med_checkup_infant(raw_data_med_checkup_infant)

        raw_data_fp_stock = raw_data_list[self.SCHEME['FP_STOCK']]
        if len(raw_data_fp_stock) > 0:
            self.preprocess_fp_stock(raw_data_fp_stock)

        raw_data_fp_bank = raw_data_list[self.SCHEME['FP_BANK']]
        if len(raw_data_fp_bank) > 0:
            self.preprocess_fp_bank(raw_data_fp_bank)

        raw_data_fp_card = raw_data_list[self.SCHEME['FP_CARD']]
        if len(raw_data_fp_card) > 0:
            self.preprocess_fp_card(raw_data_fp_card)

        raw_data_fp_insurance = raw_data_list[self.SCHEME['FP_INSURANCE']]
        if len(raw_data_fp_insurance) > 0:
            self.preprocess_fp_insurance(raw_data_fp_insurance)

        unidentified_data = raw_data_list['UNIDENTIFIED']
        if len(unidentified_data) > 0:
            file_name = self.get_current_file_name()
            file_path = f'{self.output_dir}/{file_name}_unidentified_data.json'

            fp = Path(file_path)
            if fp.is_file():
                with open(file_path) as f:
                    data = ujson.load(f)
                    data += unidentified_data
            else:
                data = unidentified_data

            with open(file_path, 'w', encoding='utf-8') as fp:
                ujson.dump(data, fp, ensure_ascii=False)
                del unidentified_data

        del raw_data_list

    def set_gender(self, df):
        df['user_gender'] = df['user_gender'].apply(lambda x: ('F' if int(x) == 0 else ('M' if int(x) == 1 else x)) if isinstance(x, int) else x)
        return df

    def replace_whitespace(self, col):
        if col is not None and not is_numeric_dtype(col):
            col = col.replace('\r|\n|\t', '', regex=True)
            col = col.replace('  ', '')
        return col

    def preprocess_fp_bank(self, raw_data_fp_bank):
        bank = self.to_dataframe(raw_data_fp_bank)

        # STEP1. data_type 컬럼(데이터 종류를 바로 알 수 있는 정보) 추가
        bank['data_type'] = self.SCHEME['FP_BANK']

        # STEP2. 성별 컬럼 숫자에서 영어로 전환(0 = F, 1 = M)
        bank = self.set_gender(bank)

        # STEP3. 컬럼 순서 정리 및 order by
        bank = bank[['data_type', 'data_created', 'user_id',
                     'company_code', 'company_name', 'is_connected', 'timestamp',
                     'account_company_code', 'account_company_name', 'account_number',
                     'account_name', 'account_type','account_status',
                     'account_is_minus', 'account_is_foreign_deposit', 'account_is_trans_memo_agreed',
                     'account_search_date', 'account_timestamp',
                     'data_category', 'basics_saving_type', 'basics_issue_date', 'basics_expire_date',
                     'basics_currency', 'basics_timestamp',
                     'details_balance', 'details_withdraw_amount', 'details_offered_rate', 'details_latest_round',
                     'details_currency', 'details_timestamp',
                     'transaction_time', 'transaction_number', 'transaction_type',
                     'transaction_class', 'transaction_amount',
                     'transaction_balance', 'transaction_memo', 'transaction_currency',
                     'user_gender', 'user_birth', 'user_region', 'user_email', 'user_installed_app',
                     'user_device_model', 'user_mobile_carrier', 'user_name', 'user_phone_number']]

        bank = bank.sort_values(by=['user_id'])
        bank = bank.reset_index(drop=True)

        self.export_csv(bank, self.SCHEME['FP_BANK'])

    def preprocess_fp_card(self, raw_data_fp_card):
        card = self.to_dataframe(raw_data_fp_card)

        # STEP1. data_type 컬럼(데이터 종류를 바로 알 수 있는 정보) 추가
        card['data_type'] = 'FPCard_account_transaction'

        # STEP2. 성별 컬럼 숫자에서 영어로 전환(0 = F, 1 = M)
        card = self.set_gender(card)

        # STEP3. 컬럼 순서 정리 및 order by
        card = card[['data_type', 'data_created', 'user_id',
                     'company_code', 'company_name', 'is_connected', 'timestamp',
                     'account_company_code', 'account_company_name', 'account_card_id', 'account_card_number',
                     'account_card_name', 'account_card_type', 'account_card_brand',
                     'account_annual_fee', 'account_issue_date', 'account_is_traffic', 'account_is_cashcard',
                     'account_is_store_number_agreed', 'account_search_date', 'account_timestamp',
                     'data_category', 'transaction_approved_number', 'transaction_approved_time',
                     'transaction_approved_status', 'transaction_payment_type',
                     'transaction_store_name', 'transaction_store_number', 'transaction_approved_amount',
                     'transaction_approved_amount_krw', 'transaction_approved_country_code',
                     'transaction_approved_currency', 'transaction_modified_time', 'transaction_modified_amount',
                     'transaction_installment_count', 'transaction_timestamp',
                     'user_gender', 'user_birth', 'user_region', 'user_email', 'user_installed_app',
                     'user_device_model', 'user_mobile_carrier', 'user_name', 'user_phone_number']]

        card = card.sort_values(by=['user_id'])
        card = card.reset_index(drop=True)

        self.export_csv(card, self.SCHEME['FP_CARD'])

    def preprocess_fp_insurance(self, raw_data_fp_insurance):
        insurance = self.to_dataframe(raw_data_fp_insurance)

        # STEP1. data_type 컬럼(데이터 종류를 바로 알 수 있는 정보) 추가
        insurance['data_type'] = self.SCHEME['FP_INSURANCE']

        # STEP2. 성별 컬럼 숫자에서 영어로 전환(0 = F, 1 = M)
        insurance = self.set_gender(insurance)

        # STEP3. 컬럼 순서 정리 및 order by
        insurance = insurance[['data_type', 'data_created', 'user_id',
                               'company_code', 'company_name', 'is_connected', 'timestamp',
                               'data_category', 'contract_company_code', 'contract_company_name',
                               'contract_policy_number', 'contract_name',
                               'contract_type', 'contract_status', 'contract_issue_date', 'contract_expire_date',
                               'contract_pay_amount',
                               'contract_pay_cycle', 'contract_pay_end_date', 'contract_pay_count',
                               'contract_krw_amount', 'contract_currency',
                               'contract_is_renewable', 'contract_is_universal', 'contract_is_variable',
                               'coverage_name', 'coverage_amount', 'coverage_currency', 'coverage_status',
                               'coverage_expire_date', 'coverage_timestamp',
                               'car_name', 'car_number', 'car_contract_age', 'car_contract_driver',
                               'car_self_pay_amount', 'car_self_pay_rate', 'car_is_own_dmg_coverage',
                               'contract_search_date', 'contract_timestamp',
                               'user_gender', 'user_birth', 'user_region', 'user_email', 'user_installed_app',
                               'user_device_model', 'user_mobile_carrier', 'user_name', 'user_phone_number']]

        insurance = insurance.sort_values(by=['user_id'])
        insurance = insurance.reset_index(drop=True)

        self.export_csv(insurance, self.SCHEME['FP_INSURANCE'])

    def preprocess_fp_stock(self, raw_data_fp_stock):
        stock = self.to_dataframe(raw_data_fp_stock)

        # STEP1. data_type 컬럼(데이터 종류를 바로 알 수 있는 정보) 추가
        stock['data_type'] = self.SCHEME['FP_STOCK']

        # STEP2. 성별 컬럼 숫자에서 영어로 전환(0 = F, 1 = M)
        stock = self.set_gender(stock)

        # STEP3. 컬럼 순서 정리 및 order by
        stock = stock[['data_type', 'data_created', 'user_id',
                       'company_code', 'company_name', 'is_connected', 'timestamp',
                       'account_company_code', 'account_company_name', 'account_number', 'account_name', 'account_type',
                       'account_issue_date', 'account_base_date', 'account_is_tax_benefits', 'account_search_date',
                       'account_timestamp',
                       'data_category', 'basics_deposit', 'basics_currency', 'basics_credit_loan_amount',
                       'basics_mortgage_amount', 'basics_timestamp',
                       'assets_product_type', 'assets_product_type_detail', 'assets_product_code',
                       'assets_product_name', 'assets_credit_type',
                       'assets_quantity', 'assets_purchase_amount', 'assets_valuation_amount', 'assets_currency',
                       'assets_timestamp',
                       'user_gender', 'user_birth', 'user_region', 'user_email', 'user_installed_app',
                       'user_device_model', 'user_mobile_carrier', 'user_name', 'user_phone_number']]

        stock = stock.sort_values(by=['user_id'])
        stock = stock.reset_index(drop=True)

        self.export_csv(stock, self.SCHEME['FP_STOCK'])

    def preprocess_med_checkup_infant(self, raw_data_med_checkup_infant):
        checkup_infant = self.to_dataframe(raw_data_med_checkup_infant)

        # 아동 검진결과 데이터 전처리
        # STEP1. data_type 컬럼(데이터 종류를 바로 알 수 있는 정보) 추가
        checkup_infant['data_type'] = self.SCHEME['MEDICAL_CHECKUP']+'_infant'

        # STEP2. opinion/analysis/etc_opinion컬럼 clean-up (불필요한 개행코드 제거 ex.\r, \t, \n 등)
        checkup_infant['opinion'] = self.replace_whitespace(checkup_infant['opinion'])
        checkup_infant['oral_analysis'] = self.replace_whitespace(checkup_infant['oral_analysis'])
        checkup_infant['oral_etc'] = self.replace_whitespace(checkup_infant['oral_etc'])

        # STEP3. 성별 컬럼 숫자에서 영어로 전환(0 = F, 1 = M)
        checkup_infant = self.set_gender(checkup_infant)

        # STEP4. 컬럼 순서 정리 및 order by
        checkup_infant = checkup_infant[['data_type', 'data_created', 'user_id',
                                         'data_category', 'checkup_date', 'checkup_target', 'resident_id',
                                         'target_type', 'checkup_type', 'organization', 'original_data',
                                         'question_info', 'opinion', 'doctor_name', 'license_number',
                                         'nursing_symbol', 'document_title', 'issue_number', 'issue_type', 'issue_purpose',
                                         'sight_questionnaire', 'sight_chart', 'sight_test', 'left_sight',
                                         'right_sight', 'hearing_questionnaire', 'development_evaluation_name',
                                         'development_evaluation_result',
                                         'oral_health_awareness', 'oral_problem_history', 'oral_problematic_habit1',
                                         'oral_problematic_habit2', 'oral_problematic_habit3', 'oral_condition',
                                         'oral_restored_teeth', 'oral_caries', 'oral_risky_caries',
                                         'oral_proximal_caries', 'oral_plague', 'oral_hygiene', 'oral_analysis',
                                         'oral_etc', 'total_judgement',
                                         'result_type', 'item', 'result', 'judgement', 'reference', 'remark',
                                         'user_gender', 'user_birth', 'user_region', 'user_email', 'user_installed_app',
                                         'user_device_model', 'user_mobile_carrier', 'user_name', 'user_phone_number']]

        checkup_infant = checkup_infant.sort_values(by=['user_id'])
        checkup_infant = checkup_infant.reset_index(drop=True)

        self.export_csv(checkup_infant, self.SCHEME['MEDICAL_CHECKUP']+'_infant')

    def preprocess_med_checkup(self, raw_data_med_checkup):
        checkup_adult = self.to_dataframe(raw_data_med_checkup)

        # 성인 검진결과 데이터 전처리
        # STEP1. data_type 컬럼(데이터 종류를 바로 알 수 있는 정보) 추가
        checkup_adult['data_type'] = self.SCHEME['MEDICAL_CHECKUP']+'_general'

        # STEP2. opinion컬럼 clean-up (불필요한 개행코드 제거 ex.\r, \t, \n 등)
        checkup_adult['opinion'] = self.replace_whitespace(checkup_adult['opinion'])

        # STEP3. 성별 컬럼 숫자에서 영어로 전환(0 = F, 1 = M)
        checkup_adult = self.set_gender(checkup_adult)

        # STEP4. 컬럼 순서 정리 및 order by
        checkup_adult = checkup_adult[['data_type', 'data_created', 'user_id',
                                       'data_category', 'category_type', 'checkup_date', 'checkup_target',
                                       'target_type', 'checkup_type', 'organization', 'original_data',
                                       'checkup_place', 'question_info', 'opinion',
                                       'height', 'weight', 'waist', 'bmi', 'sight', 'left_sight', 'right_sight',
                                       'hearing', 'left_hearing', 'right_hearing',
                                       'blood_pressure', 'high_blood_pressure', 'low_blood_pressure',
                                       'total_cholesterol', 'hdl_cholesterol', 'ldl_cholesterol', 'hemoglobin',
                                       'fasting_blood_glucose', 'urinary_protein', 'triglyceride', 'serum_creatinine',
                                       'gfr', 'ast', 'alt', 'ygpt', 'tb_chest_disease', 'osteoporosis', 'judgement',
                                       'user_gender', 'user_birth', 'user_region', 'user_email', 'user_installed_app',
                                       'user_device_model', 'user_mobile_carrier', 'user_name', 'user_phone_number']]

        checkup_adult = checkup_adult.sort_values(by=['user_id'])
        checkup_adult = checkup_adult.reset_index(drop=True)

        self.export_csv(checkup_adult, self.SCHEME['MEDICAL_CHECKUP']+'_general')

    def preprocess_med_record(self, raw_data_med_record):
        record = self.to_dataframe(raw_data_med_record)

        # STEP1. data_type 컬럼(데이터 종류를 바로 알 수 있는 정보) 추가
        record['data_type'] = self.SCHEME['MEDICAL_RECORD']

        # STEP2. 성별 컬럼 숫자에서 영어로 전환(0 = F, 1 = M)
        record = self.set_gender(record)

        # STEP3. 컬럼 순서 정리 및 order by
        record = record[['data_type', 'data_created', 'user_id',
                         'patient_type', 'treatment_type', 'treatment_start_date', 'hospital_name', 'visit_days',
                         'num_medications', 'num_prescriptions',
                         'medicine_treatment_type', 'medicine_treatment_date', 'medicine_medication_days',
                         'medicine_effect', 'medicine_name', 'medicine_num_prescriptions',
                         'user_gender', 'user_birth', 'user_region', 'user_email', 'user_installed_app',
                         'user_device_model', 'user_mobile_carrier', 'user_name', 'user_phone_number']]

        record = record.sort_values(by=['user_id'])
        record = record.reset_index(drop=True)

        self.export_csv(record, self.SCHEME['MEDICAL_RECORD'])

    def preprocess_search(self, raw_data_search):
        search = self.to_dataframe(raw_data_search)

        # 검색데이터 전처리
        # STEP1. data_type 컬럼(데이터 종류를 바로 알 수 있는 정보) 추가
        search['data_type'] = self.SCHEME['KEYWORD_ENGINE']

        # STEP2. 키워드 clean-up (불필요한 개행코드 제거 ex.\r, \t, \n 등)
        search['keyword'] = self.replace_whitespace(search['keyword'])

        # STEP3. 성별 컬럼 숫자에서 영어로 전환(0 = F, 1 = M)
        search = self.set_gender(search)

        # STEP4. 컬럼 순서 정리 및 order by
        search = search[['data_type', 'data_created', 'user_id',
                         'platform_type', 'platform_name', 'is_connected',
                         'use_date', 'keyword', 'image_url', 'engine_type', 'service_type', 'use_type',
                         'user_gender', 'user_birth', 'user_region', 'user_email', 'user_installed_app',
                         'user_device_model', 'user_mobile_carrier', 'user_name', 'user_phone_number']]

        search = search.sort_values(by=['user_id'])
        search = search.reset_index(drop=True)

        self.export_csv(search, self.SCHEME['KEYWORD_ENGINE'])

    def preprocess_shopping(self, raw_data_shopping):
        shopping = self.to_dataframe(raw_data_shopping)

        # option 일괄 처리
        shopping.loc[shopping['details_unit_option'] == '', 'details_unit_option'] = None

        # STEP1. data_type 컬럼(데이터 종류를 바로 알 수 있는 정보) 추가
        shopping['data_type'] = self.SCHEME['OPENMARKET']

        # STEP2. 상품명 clean-up (불필요한 개행코드 제거 ex.\r, \t, \n 등)
        shopping['details_status'] = self.replace_whitespace(shopping['details_status'])
        shopping['details_unit_name'] = self.replace_whitespace(shopping['details_unit_name'])
        shopping['details_unit_option'] = self.replace_whitespace(shopping['details_unit_option'])

        # STEP3. 성별 컬럼 숫자에서 영어로 전환(0 = F, 1 = M)
        shopping = self.set_gender(shopping)

        # STEP4. 컬럼 순서 정리 및 order by
        shopping = shopping[['data_type', 'data_created', 'user_id',
                             'open_market_type', 'open_market_name', 'is_connected',
                             'order_market_type', 'order_number', 'order_date',
                             'order_payment_total', 'order_shipping_cost', 'order_saved_amount', 'order_amount',
                             'details_status', 'details_is_cancelled',
                             'details_unit_price', 'details_unit_qty', 'details_unit_amount', 'details_unit_name',
                             'details_unit_option',
                             'category1', 'category2', 'category3', 'category4',
                             'user_gender', 'user_birth', 'user_region', 'user_email', 'user_installed_app',
                             'user_device_model', 'user_mobile_carrier', 'user_name', 'user_phone_number']]

        shopping = shopping.sort_values(by=['user_id'])
        shopping = shopping.reset_index(drop=True)

        self.export_csv(shopping, self.SCHEME['OPENMARKET'])

    def get_current_file_name(self):
        return self.output_file_name if self.merge else self.current_file_name.split('/')[len(self.current_file_name.split('/')) - 1].split('.json')[0]

    def export_csv(self, df, scheme):
        file_name = self.get_current_file_name()
        file_num = 0

        res = self.check_file_row(scheme, file_name)
        # 앞에서 수행된 적이 없다면
        if res is None:
            saved_row_count, file_num = self.seperate_file_by_max_unit(df, file_name, scheme, file_num)
        else:
            file_name, scheme, file_num, row_count = res

            # if 앞의 row와 현재 df의 row 합이 max_row 미만이면 앞의 파일 열어서 저장
            current_rows = len(df.index)
            if row_count + current_rows <= CONF['MAX_CSV_ROW']:
                file_path = f'{self.output_dir}/{file_name}_{scheme}_{str(file_num)}'
                df.to_csv(f'{file_path}.csv', mode='a', header=False, sep=',', na_rep='', quoting=csv.QUOTE_ALL, index=False, encoding="utf-8-sig")
                saved_row_count = current_rows + row_count

            # else 앞의 파일 채우고 나머지는 새 파일에 저장
            else:
                # ex) current:351, row_count:894, max:1000 인 경우
                # ex) 1000 - 894 = 106 이므로 current를 [0:106], [106:]으로 각각 저장
                split_size = int(CONF['MAX_CSV_ROW'] - row_count)
                first_half = df[0:split_size]
                second_half = df[split_size:]

                file_path = f'{self.output_dir}/{file_name}_{scheme}_{str(file_num)}'
                first_half.to_csv(f'{file_path}.csv', mode='a', header=False, sep=',', na_rep='', quoting=csv.QUOTE_ALL, index=False, encoding="utf-8-sig")

                saved_row_count, file_num = self.seperate_file_by_max_unit(second_half, file_name, scheme, file_num)

        self.set_csv_rows(scheme, file_name, file_num, saved_row_count)
        del df

    def seperate_file_by_max_unit(self, df, file_name, scheme, file_num):
        df_sep = df

        # 현재 df의 크기가 max_row를 넘는다면 분할해서 저장
        current_rows = len(df)
        repeat_num = int(current_rows / CONF['MAX_CSV_ROW']) + 1

        for i in range(0, repeat_num):
            if i == repeat_num - 1:
                df_sep = df[i * CONF['MAX_CSV_ROW']:]
            else:
                df_sep = df[i * CONF['MAX_CSV_ROW']:(i + 1) * CONF['MAX_CSV_ROW']]

            file_num += 1
            file_path = f'{self.output_dir}/{file_name}_{scheme}_{file_num}'
            df_sep.to_csv(f'{file_path}.csv', sep=',', na_rep='', quoting=csv.QUOTE_ALL, index=False, encoding="utf-8-sig")

        return len(df_sep), file_num

    def check_file_row(self, scheme, file_name):
        try:
        conn = sqlite3.connect(CONF['CSV_STATUS_DB'])
        c = conn.cursor()
        c.execute("""select file_name, scheme, file_num, row_count from csv_rows 
                        where job_id = ? 
                        and scheme = ?
                        and file_name = ?
                        order by file_num desc""", [self.job_id, scheme, file_name])
        res = c.fetchone()
        c.close()
        conn.close()
        return res
        except sqlite3.Error as error:
        print('*** Failed to execute a query.', error)
        finally:
        if conn:
            conn.close()
            print('SQLite connection is closed') 

    def set_csv_rows(self, scheme, file_name, file_num, row_count):
        try:
        conn = sqlite3.connect(CONF['CSV_STATUS_DB'])
        c = conn.cursor()
        current_datetime = datetime.now()
        c.execute("""INSERT OR REPLACE INTO csv_rows (job_id, scheme, file_name, file_num, row_count, update_datetime) 
                        VALUES (?, ?, ?, ?, ?, ?);""", [self.job_id, scheme, file_name, file_num, row_count, current_datetime])
        conn.commit()
        c.close()
        conn.close()
        except sqlite3.Error as error:
        print('*** Failed to execute a query.', error)
        finally:
        if conn:
            conn.close()
            print('SQLite connection is closed')

    def has_array(self, key, arr):
        return key in arr and len(arr[key]) > 0

    def to_dataframe(self, list):
        df = pd.DataFrame(list)
        df = df.astype('object')
        df = df.where(pd.notnull(df), None)
        del list
        return df

    def find_stock_account(self, row_obj_account, account):
        total_data_account = []

        row_obj_account['is_connected'] = account.get('isConnected', None)
        row_obj_account['account_company_code'] = account.get('companyCode', None)
        row_obj_account['account_company_name'] = account.get('companyName', None)
        row_obj_account['account_number'] = account.get('accountNumber', None)
        row_obj_account['account_name'] = account.get('accountName', None)
        row_obj_account['account_type'] = account.get('accountType', None)
        row_obj_account['account_issue_date'] = account.get('issueDate', None)
        row_obj_account['account_base_date'] = account.get('baseDate', None)
        row_obj_account['account_is_tax_benefits'] = account.get('isTaxBenefits', None)
        row_obj_account['account_search_date'] = account.get('searchDate', None)
        row_obj_account['account_timestamp'] = account.get('timestamp', None)

        if self.has_array('basics', account) or self.has_array('assets', account):
            if self.has_array('basics', account):
                basics = account['basics']

                for basic in basics:
                    row_obj_basic = pickle.loads(pickle.dumps(row_obj_account))

                    row_obj_basic['data_category'] = 'Basics'
                    row_obj_basic['basics_deposit'] = basic.get('deposit', None)
                    row_obj_basic['basics_currency'] = basic.get('currency', None)
                    row_obj_basic['basics_credit_loan_amount'] = basic.get('creditLoanAmount', None)
                    row_obj_basic['basics_mortgage_amount'] = basic.get('mortgageAmount', None)
                    row_obj_basic['basics_timestamp'] = basic.get('timestamp', None)

                    total_data_account.append(row_obj_basic)

            if self.has_array('assets', account):
                assets = account['assets']

                for asset in assets:
                    row_obj_asset = pickle.loads(pickle.dumps(row_obj_account))

                    row_obj_asset['data_category'] = 'Assets'
                    row_obj_asset['assets_product_type'] = asset.get('productType', None)
                    row_obj_asset['assets_product_type_detail'] = asset.get('productTypeDetail', None)
                    row_obj_asset['assets_product_code'] = asset.get('productCode', None)
                    row_obj_asset['assets_product_name'] = asset.get('productName', None)
                    row_obj_asset['assets_credit_type'] = asset.get('creditType', None)
                    row_obj_asset['assets_quantity'] = asset.get('quantity', None)
                    row_obj_asset['assets_purchase_amount'] = asset.get('purchaseAmount', None)
                    row_obj_asset['assets_valuation_amount'] = asset.get('valuationAmount', None)
                    row_obj_asset['assets_currency'] = asset.get('currency', None)
                    row_obj_asset['assets_timestamp'] = asset.get('timestamp', None)

                    total_data_account.append(row_obj_asset)

        else:
            # basics, assets 모두 없는 경우
            # basics, assets 모두 빈 데이터([])인 경우
            total_data_account.append(row_obj_account)

        return total_data_account

    def find_bank_account(self, row_obj_account, account):
        total_data_account = []

        row_obj_account['account_company_code'] = account.get('companyCode', None)
        row_obj_account['account_company_name'] = account.get('companyName', None)
        row_obj_account['account_number'] = account.get('accountNumber', None)
        row_obj_account['account_name'] = account.get('accountName', None)
        row_obj_account['account_type'] = account.get('accountType', None)
        row_obj_account['account_status'] = account.get('accountStatus', None)
        row_obj_account['account_is_minus'] = account.get('isMinus', None)
        row_obj_account['account_is_foreign_deposit'] = account.get('isForeignDeposit', None)
        row_obj_account['account_is_trans_memo_agreed'] = account.get('isTransMemoAgreed', None)
        row_obj_account['account_search_date'] = account.get('searchDate', None)
        row_obj_account['account_timestamp'] = account.get('timestamp', None)

        if self.has_array('basics', account) or self.has_array('details', account) or self.has_array('transactions', account):
            if self.has_array('basics', account):
                basics = account['basics']

                for basic in basics:
                    row_obj_basic = pickle.loads(pickle.dumps(row_obj_account))

                    row_obj_basic['data_category'] = 'Basics'
                    row_obj_basic['basics_saving_type'] = basic.get('savingType', None)
                    row_obj_basic['basics_issue_date'] = basic.get('issueDate', None)
                    row_obj_basic['basics_expire_date'] = basic.get('expireDate', None)
                    row_obj_basic['basics_currency'] = basic.get('currency', None)
                    row_obj_basic['basics_timestamp'] = basic.get('timestamp', None)

                    total_data_account.append(row_obj_basic)

            if self.has_array('details', account):
                details = account['details']

                for detail in details:
                    row_obj_detail = pickle.loads(pickle.dumps(row_obj_account))

                    row_obj_detail['data_category'] = 'Details'
                    row_obj_detail['details_balance'] = detail.get('balance', None)
                    row_obj_detail['details_withdraw_amount'] = detail.get('withdrawAmount', None)
                    row_obj_detail['details_offered_rate'] = detail.get('offeredRate', None)
                    row_obj_detail['details_latest_round'] = detail.get('latestRound', None)
                    row_obj_detail['details_currency'] = detail.get('currency', None)
                    row_obj_detail['details_timestamp'] = detail.get('timestamp', None)

                    total_data_account.append(row_obj_detail)

            if self.has_array('transactions', account):
                transactions = account['transactions']

                for transaction in transactions:
                    row_obj_transaction = pickle.loads(pickle.dumps(row_obj_account))

                    row_obj_transaction['data_category'] = 'Transactions'
                    row_obj_transaction['transaction_time'] = transaction.get('transactionTime', None)
                    row_obj_transaction['transaction_number'] = transaction.get('transactionNumber', None)
                    row_obj_transaction['transaction_type'] = transaction.get('transactionType', None)
                    row_obj_transaction['transaction_class'] = transaction.get('transactionClass', None)
                    row_obj_transaction['transaction_amount'] = transaction.get('amount', None)
                    row_obj_transaction['transaction_balance'] = transaction.get('balance', None)
                    row_obj_transaction['transaction_memo'] = transaction.get('memo', None)
                    row_obj_transaction['transaction_currency'] = transaction.get('currency', None)

                    total_data_account.append(row_obj_transaction)

        else:
            # basics, details, transactions 모두 없는 경우
            total_data_account.append(row_obj_account)

        return total_data_account

    def find_card_account(self, row_obj_account, account):
        total_data_account = []

        row_obj_account['account_company_code'] = account.get('companyCode', None)
        row_obj_account['account_company_name'] = account.get('companyName', None)
        row_obj_account['account_card_id'] = account.get('cardId', None)
        row_obj_account['account_card_number'] = account.get('cardNumber', None)
        row_obj_account['account_card_name'] = account.get('cardName', None)
        row_obj_account['account_card_type'] = account.get('cardType', None)
        row_obj_account['account_card_brand'] = account.get('cardBrand', None)
        row_obj_account['account_annual_fee'] = account.get('annualFee', None)
        row_obj_account['account_issue_date'] = account.get('issueDate', None)
        row_obj_account['account_is_traffic'] = account.get('isTraffic', None)
        row_obj_account['account_is_cashcard'] = account.get('isCashCard', None)
        row_obj_account['account_is_store_number_agreed'] = account.get('isStoreNumberAgreed', None)
        row_obj_account['account_search_date'] = account.get('searchDate', None)
        row_obj_account['account_timestamp'] = account.get('timestamp', None)

        if self.has_array('domesticTransactions', account) or self.has_array('foreignTransactions', account):
            for subIdentifier in ['domesticTransactions', 'foreignTransactions']:
                if self.has_array(subIdentifier, account):
                    transactions = account[subIdentifier]

                    for transaction in transactions:
                        row_obj_transaction = pickle.loads(pickle.dumps(row_obj_account))

                        if subIdentifier == 'domesticTransactions':
                            row_obj_transaction['data_category'] = 'DomesticTransactions'
                        elif subIdentifier == 'foreignTransactions':
                            row_obj_transaction['data_category'] = 'ForeignTransactions'

                        row_obj_transaction['transaction_approved_number'] = transaction.get('approvedNumber', None)
                        row_obj_transaction['transaction_approved_time'] = transaction.get('approvedTime', None)
                        row_obj_transaction['transaction_approved_status'] = transaction.get('approvedStatus', None)
                        row_obj_transaction['transaction_payment_type'] = transaction.get('paymentType', None)
                        row_obj_transaction['transaction_store_name'] = transaction.get('storeName', None)
                        row_obj_transaction['transaction_store_number'] = transaction.get('storeNumber', None)
                        row_obj_transaction['transaction_approved_amount'] = transaction.get('approvedAmount', None)
                        row_obj_transaction['transaction_approved_amount_krw'] = transaction.get('krwAmount', None)
                        row_obj_transaction['transaction_approved_country_code'] = transaction.get('countryCode', None)
                        row_obj_transaction['transaction_approved_currency'] = transaction.get('currency', None)
                        row_obj_transaction['transaction_modified_time'] = transaction.get('modifiedTime', None)
                        row_obj_transaction['transaction_modified_amount'] = transaction.get('modifiedAmount', None)
                        row_obj_transaction['transaction_installment_count'] = transaction.get('installmentCount', None)
                        row_obj_transaction['transaction_timestamp'] = transaction.get('timestamp', None)

                        total_data_account.append(row_obj_transaction)

        else:
            # domesticTransactions, foreignTransactions 모두 없는 경우
            total_data_account.append(row_obj_account)

        return total_data_account

    def find_insurance_contract(self, row_obj_contract, contract, scheme):
        total_data_contract = []

        if scheme == 'FPInsuranceContract':
            row_obj_contract['data_category'] = 'Contract'
            row_obj_contract['contract_company_code'] = contract.get('companyCode', None)
            row_obj_contract['contract_company_name'] = contract.get('companyName', None)
            row_obj_contract['contract_policy_number'] = contract.get('policyNumber', None)
            row_obj_contract['contract_name'] = contract.get('contractName', None)
            row_obj_contract['contract_type'] = contract.get('contractType', None)
            row_obj_contract['contract_status'] = contract.get('contractStatus', None)
            row_obj_contract['contract_issue_date'] = contract.get('issueDate', None)
            row_obj_contract['contract_expire_date'] = contract.get('expireDate', None)
            row_obj_contract['contract_pay_amount'] = contract.get('payAmount', None)
            row_obj_contract['contract_pay_cycle'] = contract.get('payCycle', None)
            row_obj_contract['contract_pay_end_date'] = contract.get('payEndDate', None)
            row_obj_contract['contract_pay_count'] = contract.get('payCount', None)
            row_obj_contract['contract_krw_amount'] = contract.get('krwAmount', None)
            row_obj_contract['contract_currency'] = contract.get('currency', None)
            row_obj_contract['contract_is_renewable'] = contract.get('isRenewable', None)
            row_obj_contract['contract_is_universal'] = contract.get('isUniversal', None)
            row_obj_contract['contract_is_variable'] = contract.get('isVariable', None)
            row_obj_contract['contract_search_date'] = contract.get('searchDate', None)
            row_obj_contract['contract_timestamp'] = contract.get('timestamp', None)

            if self.has_array('coverages', contract):
                coverages = contract['coverages']

                for coverage in coverages:
                    row_obj_coverage = pickle.loads(pickle.dumps(row_obj_contract))

                    row_obj_coverage['coverage_name'] = coverage.get('coverageName', None)
                    row_obj_coverage['coverage_amount'] = coverage.get('coverageAmount', None)
                    row_obj_coverage['coverage_currency'] = coverage.get('currency', None)
                    row_obj_coverage['coverage_status'] = coverage.get('coverageStatus', None)
                    row_obj_coverage['coverage_expire_date'] = coverage.get('expireDate', None)
                    row_obj_coverage['coverage_timestamp'] = coverage.get('timestamp', None)

                    total_data_contract.append(row_obj_coverage)

            else:
                # coverages 정보 없는 케이스
                total_data_contract.append(row_obj_contract)

        elif scheme == 'FPInsuranceCarContract':
            row_obj_contract['data_category'] = 'CarContract'
            row_obj_contract['contract_company_code'] = contract.get('companyCode', None)
            row_obj_contract['contract_company_name'] = contract.get('companyName', None)
            row_obj_contract['contract_policy_number'] = contract.get('policyNumber', None)
            row_obj_contract['contract_type'] = contract.get('contractType', None)
            row_obj_contract['contract_status'] = contract.get('contractStatus', None)
            row_obj_contract['contract_issue_date'] = contract.get('issueDate', None)
            row_obj_contract['contract_expire_date'] = contract.get('expireDate', None)
            row_obj_contract['contract_pay_amount'] = contract.get('payAmount', None)
            row_obj_contract['contract_pay_count'] = contract.get('payCount', None)
            row_obj_contract['contract_search_date'] = contract.get('searchDate', None)
            row_obj_contract['contract_timestamp'] = contract.get('timestamp', None)
            row_obj_contract['car_name'] = contract.get('carName', None)
            row_obj_contract['car_number'] = contract.get('carNumber', None)
            row_obj_contract['car_contract_age'] = contract.get('contractAge', None)
            row_obj_contract['car_contract_driver'] = contract.get('contractDriver', None)
            row_obj_contract['car_self_pay_amount'] = contract.get('selfPayAmount', None)
            row_obj_contract['car_self_pay_rate'] = contract.get('selfPayRate', None)
            row_obj_contract['car_is_own_dmg_coverage'] = contract.get('isOwnDmgCoverage', None)

            total_data_contract.append(row_obj_contract)

        return total_data_contract
