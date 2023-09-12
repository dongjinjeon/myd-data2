from flask import Flask, make_response, request
from flask_restx import Resource, Namespace, Api, fields
from preprocess import Preprocess
from analysis import Analysis
from convert_to_csv import ConvertToCSV
from check_status import CheckStatus
import json
import uuid
from threading import Thread
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

RAW_DATA_DIR = 'data'
PROPOSAL_ID = ''
USER_ID = ''
FILE_NAME = 'myd_sample.json'

app = Flask(__name__)
api = Api(
    app,
    version='1.0',
    title="My:D analysis restful api",
    description="데이터마켓 raw data 분석을 위한 python restful api server documentaion 입니다.",
    terms_url="/",
    contact="sky@searcheese.com"
)
scheduler = BackgroundScheduler()
scheduler.start()

ns_shopping_preprocess = Namespace('preprocess', description='데이터 분석 전에 전처리를 위해 사용하는 API')
api.add_namespace(ns_shopping_preprocess)

ns_shopping_analysis = Namespace('analysis', description='전처리가 완료된 데이터 분석 API')
api.add_namespace(ns_shopping_analysis)

ns_convert_to_csv = Namespace('convert_to_csv', description='JSON 데이터를 CSV로 변환하여 저장하는 API')
api.add_namespace(ns_convert_to_csv)

ns_version = Namespace('version', description='Server version')
api.add_namespace(ns_version)

shopping_prc_res_fields = ns_shopping_preprocess.model('Shopping preprocess response', {  # Model 객체 생성
    'data': fields.Nested(ns_shopping_preprocess.model('data', {
        'months': fields.List(fields.String(description='Month', required=True, example="08")),
        'categories': fields.Nested(
            ns_shopping_preprocess.model('Category1', {
                "*": fields.Wildcard(
                    fields.List(fields.String(description='Category2', example=["냉동/간편조리식품","건강식품","축산물"])),
                    description='Category1', example='식품'
                )
            }),
            example=[{'식품': ["냉동/간편조리식품","건강식품","축산물"]},{'생활/건강': ["생활용품","건강관리용품","구강위생용품"]}]
        ),
    }))
})

@ns_shopping_preprocess.route('/shopping')
@ns_shopping_preprocess.doc(params={
    'proposal_id': {'in': 'query', 'description': '공고 Id', 'required': 'true', 'type': 'string', 'default': 'sample_proposal_id'},
    'user_id': {'in': 'query', 'description': 'Biz 사용자 Id', 'required': 'true', 'type': 'string', 'default': 'sample_user_id'},
    'file_name': {'in': 'query', 'description': 'Docker /data 디렉토리에 바인딩된 폴더 하위의 파일명을 포함한 분석 대상 파일 경로', 'required': 'true', 'type': 'string', 'default': '/app/data/encrypted.myd_sample.json'},
    'aes256cbckey': {'in': 'query', 'description': '파일 복호화 key', 'required': 'true', 'type': 'string', 'default': '894d2fce882339606bf0788d5576843f0b9773ff6b1f8f8a5a9290cc876155a7'},
    'iv': {'in': 'query', 'description': '파일 복호화 iv', 'required': 'true', 'type': 'string', 'default': '624cce1ae8d045fe4f3c6869f728ead3'}
})
class ShoppingPreprocessRoute(Resource):
    @ns_shopping_preprocess.response(200, 'Success', shopping_prc_res_fields)
    def get(self):
        """Shopping 데이터 전처리 후 분석할 카테고리와 월(month) 목록을 가져옵니다."""
        data_type = 'shopping'
        try:
            proposal_id = request.args.get('proposal_id')
            user_id = request.args.get('user_id')
            file_name = request.args.get('file_name')
            aes256cbckey = request.args.get('aes256cbckey')
            iv = request.args.get('iv')
            if file_name is not None and not file_name.startswith('/'):
                file_name = '/' + file_name
        except:
            raise Exception('Wrong query parameter.')

        preprocess = Preprocess(data_type, proposal_id, user_id, RAW_DATA_DIR + file_name, aes256cbckey, iv)
        data = preprocess.run()
        return set_res(data)

@ns_shopping_analysis.route('/shopping')
@ns_shopping_analysis.doc(params={
    'proposal_id': {'in': 'query', 'description': '공고 Id', 'required': 'true', 'type': 'string', 'default': 'sample_proposal_id'},
    'user_id': {'in': 'query', 'description': 'Biz 사용자 Id', 'required': 'true', 'type': 'string', 'default': 'sample_user_id'},
    'cate1': {'in': 'query', 'description': '사용자가 선택한 상위 카테고리', 'required': 'true', 'type': 'string', 'default': '식품'},
    'cate2': {'in': 'query', 'description': '사용자가 선택한 하위 카테고리', 'required': 'true', 'type': 'string', 'default': '냉동/간편조리식품'},
    'month': {'in': 'query', 'description': '사용자가 선택한 분석 대상 기간(월)', 'required': 'true', 'type': 'string', 'default': '07'}
})
class ShoppingAnalysisRoute(Resource):
    def get(self):
        """전처리 된 Shopping 데이터를 분석하여 분석 결과를 가져옵니다."""
        try:
            proposal_id = request.args.get('proposal_id')
            user_id = request.args.get('user_id')
            cate1 = request.args.get('cate1')
            cate2 = request.args.get('cate2')
            month = request.args.get('month')
            month = f"{int(month):02d}"
        except:
            raise Exception('Wrong query parameter.')
    
        analysis = Analysis(proposal_id, user_id, cate1, cate2, month)
        data = analysis.run()
        return set_res(data)

ns_convert_to_csv_model = ns_convert_to_csv.model('ConverToCSV', {
    'file_names': fields.List(
        fields.String,
        required=True,
        description='Docker /data 디렉토리에 바인딩된 폴더 하위의 파일명을 포함한 분석 대상 파일 경로',
        default=['/app/data/encrypted.myd_sample_1.json', '/app/data/encrypted.myd_sample_2.json', '/app/data/encrypted.myd_sample_3.json', '/app/data/encrypted.myd_sample_4.json']
    ),
    'output_file_name': fields.String(
        required=False,
        description='저장할 csv 파일명 (확장자 제외)',
        default='encrypted.myd_sample'
    ),
    'merge': fields.Boolean(
        required=False,
        description='csv 파일을 통합할지, file_names에 맞춰 분할할지 여부(True: ouput_file_name으로 단일 csv 파일 생성, False: file_names의 개수만큼 csv 생성)',
        default=True
    ),
    'aes256cbckey': fields.String(
        required=True,
        description='파일 복호화 key',
        default='894d2fce882339606bf0788d5576843f0b9773ff6b1f8f8a5a9290cc876155a7'
    ),
    'iv': fields.String(
        required=True,
        description='파일 복호화 iv',
        default='624cce1ae8d045fe4f3c6869f728ead3'
    )
})
@ns_convert_to_csv.route('')
class ConvertToCSVRoute(Resource):
    @ns_convert_to_csv.doc('convertToCSV_doc', body=ns_convert_to_csv_model)
    def post(self):
        """저장된 JSON 파일을 추출하여 전처리 후 CSV로 변환하여 저장합니다."""
        job_id = str(uuid.uuid4())

        try:
            body = request.get_json()
            file_names = body['file_names']
            aes256cbckey = body['aes256cbckey']
            iv = body['iv']

            # merge default 값으로 True 세팅
            merge = body['merge'] if 'merge' in body else True
            # output_file_name default 값으로 file_names 첫번째 파일명 세팅
            output_file_name = body['output_file_name'] if 'output_file_name' in body else file_names[0].split('/')[len(file_names[0].split('/')) - 1].split('.json')[0]
        except:
            raise Exception('Wrong request body.')

        convert_to_csv = ConvertToCSV(file_names, aes256cbckey, iv, job_id, output_file_name, merge)
        convert_to_csv.update_status('WORKING', '')

        # API response 후에 작업은 thread로 백그라운드에서 별도로 수행
        thread = Thread(target=background_task, args=(convert_to_csv,))
        thread.daemon = True
        thread.start()

        return set_res({"job_id": job_id})

def background_task(convert_to_csv):
    convert_to_csv.start()

@ns_convert_to_csv.route('/status')
@ns_convert_to_csv.doc(params={
    'job_id': {'in': 'query', 'description': 'convert_to_csv API의 결과로 return 된 작업의 job_id', 'required': 'true', 'type': 'string', 'default': '12eb1511-605a-4896-a4d6-1b4b2bed2172'},
})
class ConvertToCSVStatusRoute(Resource):
    def get(self):
        """수행 중인 csv 변환 작업의 상태를 조회합니다.."""
        job_id = request.args.get('job_id')
        check_status = CheckStatus()
        res = check_status.get_current_status(job_id)

        if res is None:
            status = 'NOTFOUND'
            msg = '해당 job_id의 작업이 존재하지 않습니다.'
            job_start_time = ''
        else:
            status, msg, job_start_time = res

        return set_res({"job_id": job_id, "status": status, "msg": msg, "job_start_time": job_start_time})

@ns_version.route('')
@ns_version.doc()
class VersionRoute(Resource):
    def get(self):
        # return set_res({"version": "1.0.0 (2023.06.05)"})
        return "1.0.0 (2023.06.05)"

def set_res(data):
    body = json.dumps({
        "data": data
    }, ensure_ascii=False)

    resp = make_response(body)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@scheduler.scheduled_job(CronTrigger(day_of_week='sun', hour='0'))
def clear_old_data():
    # 매주 일요일 자정에 db에 쌓인 일주일 이전 데이터 삭제
    CheckStatus().clear_old_data()
    pass

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=9090, use_reloader=False)
