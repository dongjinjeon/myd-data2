# myd-py-analysis



## Docker 수행

```
# docker 이미지 빌드
docker build -t mydimage .

# 생성된 이미지로 컨테이너 수행
docker run -d --name mydcontainer -p 9090:9090 -v /host/directory:/app/data mydimage
```

-v 옵션을 통해 host의 디렉토리와 컨테이너의 /data 디렉토리를 바인딩. 

"/host/directory" 자리에 raw data json 파일이 있는 디렉토리를 넣으면 됨.

해당 디렉토리가 프로젝트 내의 /data 디렉토리로 바인딩 되어 api 수행시 /data에서 파일을 찾게 됨.
    
## 기타 Docker 옵션 
```
# docker 컨테이너 내부 조회 
docker exec -t -i mydcontainer /bin/bash

# 컨테이너 로그 조회 
docker logs mydcontainer

# 컨테이너 종료
docker stop mydcontainer

# 컨테이너 삭제 
docker rm mydcontainer
```

## (config.py) JSON to CSV 작업 사이즈 조정
[FILE_SEP_SIZE]

FILE_SEP_SIZE 값으로 한번에 로드해서 변환할 json object 개수(id 개수)를 지정할 수 있음.

ex) FILE_SEP_SIZE가 1000인 경우 JSON 파일을 열어 json object를 한번에 1000개씩만 가져와서 변환하는 방식

혹시 JSON 사이즈가 너무 커서 서버 메모리에서 처리하지 못하는 경우를 대비하기 위함.

JSON 사이즈로 인해 out of memory 등의 서버 이슈가 발생한다면 FILE_SEP_SIZE를 줄여서 한번에 처리하는 개수를 조정하는 방식으로 세팅.


[MAX_CSV_ROW]

CSV 파일 하나에 저장할 max row를 조정할 수 있음. Default는 300,000 row.


## (csv_status.db) CSV 변환 작업의 상태 저장 db
sqlite3 db 파일에 작업중인 상태를 저장/조회 하여 속도 개선

[csv_rows table]

FILE_SEP_SIZE를 통해 분할 작업 시에, 앞의 작업에서 저장한 csv 파일의 row 수를 기록하여 csv 파일을 열지 않고도 max row를 넘었는지 확인할 수 있도록 하는 테이블.

[job_status table]

/convert_to_csv API를 통해 변환 작업이 실행되고 나면 API는 응답 후에 백그라운드에서 변환 작업을 수행하게 되는데, 이때 수행 중인 작업의 상태를 업데이트하는 테이블.

작업 실행시 생성된 job_id로 /convert_to_csv/status API를 조회하게 되면 해당 작업의 상태를 WORKING / DONE / FAILED / NOTFOUND 4가지의 상태로 응답하게됨.

[테이블 비우기]

main.py에 등록된 scheduler에서 매주 일요일 자정에 일주일 이전의 데이터를 삭제함.  

  

    
## Swagger UI
브라우저에서 127.0.01:9090 주소로 접속시 Swagger API documentation 확인 가능.


## API request sample
```
# Shopping 전처리 API request sample
GET) http://localhost:9090/preprocess/shopping?user_id=some_unisque_user_id&proposal_id=some_unique_proposal_id&file_name=/myd_sample.json

# Response sample
{
    "data": {
        "months": [
            "06",
            "07",
            "08",
            # ... 중략
        ],
        "categories": {
            "생활/건강": [
                "생활용품",
                "건강관리용품",
                "구강위생용품"
            ],
            "식품": [
                "냉동/간편조리식품",
                "건강식품",
                "축산물"
            ],
            # ... 중략
        }
    }
}


# JSON to CSV 변환 API sample 
POST) http://localhost:9090/convert_to_excel

# Request sample
{
    "file_names": [
        '/app/data/encrypted.myd_sample_1.json',
        '/app/data/encrypted.myd_sample_2.json',
        '/app/data/encrypted.myd_sample_3.json',
        '/app/data/encrypted.myd_sample_4.json',
    ],
    "output_file_name": "csv_file_name",
    "merge": True,
    "aes256cbckey": "894d2fce882339606bf0788d5576843f0b9773ff6b1f8f8a5a9290cc876155a7",
    "iv": "624cce1ae8d045fe4f3c6869f728ead3"
}

# Response sample
{
  "data": {
    "job_id": "8c2ff636-d421-4c61-9e13-42f22ccdcf48"
  }
}


# JSON to CSV 변환 작업 상태 조회 API sample
GET) http://localhost:9090/convert_to_csv/status?job_id=8c2ff636-d421-4c61-9e13-42f22ccdcf48

# Response sample 1
{
  "data": {
    "job_id": "8c2ff636-d421-4c61-9e13-42f22ccdcf48",
    "status": "DONE",
    "msg": "",
    "job_start_time": 1684224139
  }
}

# Response sample 2
{
  "data": {
    "job_id": "8c2ff636-d421-4c61-9e13-42f22ccdcf48",
    "status": "FAILED",
    "msg": "Traceback (most recent call last):\n  File \"/app/./convert_to_csv.py\", line 961, in start\n    self.run()\n  File \"/app/./convert_to_csv.py\", line 968, in run\n    json_size = self.read_json_size()\n  File \"/app/./convert_to_csv.py\", line 414, in read_json_size\n    with open(self.file_name, 'rb') as file:\nFileNotFoundError: [Errno 2] No such file or directory: '/data/fp_stock_230322.json'\n",
    "job_start_time": 1684224139
  }
}
```