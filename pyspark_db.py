import pymysql.cursors
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, to_timestamp
import json

spark = SparkSession.builder.appName("LogToMariaDB").getOrCreate()

#home = os.path.expanduser("~")
log_file = f"/home/ubuntu/log/team2chat_messages.log"
OFFSET_FILE = f"/home/ubuntu/log/offset.txt"

# 데이터 베이스 연결
def get_connection():
    connection = pymysql.connect(host=os.getenv("DB_IP", "43.201.63.84"),
                            user='user',
                            password='1234',
                            port = int(os.getenv("MY_PORT", "6033")),
                            database='encore',
                            cursorclass=pymysql.cursors.DictCursor)
    return connection

# offset 파일 생성 함수 (초기값 0)
def initialize_offset():
    if not os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, 'w') as file:
            file.write('0')

# offset 읽기 함수
def read_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, 'r') as file:
            return int(file.read().strip())
    return None

# offset 저장 함수
def save_offset(offset):
    with open(OFFSET_FILE, 'w') as file:
        file.write(str(offset))

# 스키마 정의
schema = StructType([
    StructField("time", TimestampType(), True),        # 채팅 시각 (TimestampType으로 변경)
    StructField("name", StringType(), True),           # 유저 이름
    StructField("message", StringType(), True),        # 채팅 내용
    StructField("clientIp", StringType(), True),       # 해당 IP
])

# json 형식의 로그 파일을 dataframe에 업로드 (괜히 csv 형식으로 변환보다는 이편이 나을듯)
# 이 방식이 되려면 위의 structfield 하고 json의 key값이 동일해야 함
log_load = spark.read.json(log_file, schema=schema)

# DB에 로그 데이터 삽입 함수
def insert(log_df):
    conn = get_connection()  # 데이터베이스 연결
    initialize_offset()
    offset = read_offset() # 현재 offset 읽기

    with conn:
        with conn.cursor() as cursor:
            # DataFrame의 각 행을 가져와서 파라미터 리스트 생성
            params = log_df.rdd.map(lambda row: (
                row.time,
                row.name,
                row.message,
                row.clientIp,
                None
            )).collect()  # collect()를 사용하여 리스트로 변환

            # 중복을 피하기 위해 txt파일에에서 현재 오프셋을 가져오고 offset 기준 이후 데이터만 삽입
            new_params = [param for i, param in enumerate(params) if i >= offset]

            # 새로운 데이터가 있으면 삽입
            if new_params:
                cursor.executemany("""
                    INSERT INTO chatlog (chat_time, username, chatting_content, chat_ip, chat_check)
                    VALUES (%s, %s, %s, %s, %s)
                """, new_params)

            # 변경 사항을 커밋
            conn.commit()

            # 마지막으로 삽입된 행의 offset 저장
            last_offset = offset + len(new_params)
            save_offset(last_offset)

# 로그 데이터 삽입
insert(log_load)
