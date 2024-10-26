import pymysql.cursors
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col
import sys

spark = SparkSession.builder.appName("LogToMariaDB").getOrCreate()
#sys.argv[1]

#home = os.path.expanduser("~")
log_file = f"/home/ubuntu/log/team2chat_messages.log"


def get_connection():
    connection = pymysql.connect(host=os.getenv("DB_IP", "localhost"),
                            user='team2_user',
                            password='team2_password',
                            port = int(os.getenv("MY_PORT", "6033")),
                            database='team2db',
                            cursorclass=pymysql.cursors.DictCursor)
    return connection

# 스키마 정의
schema = StructType([
    StructField("offset", StringType(), True),              # 오프셋
    StructField("chat_time", TimestampType(), True),        # 채팅 시각 (TimestampType으로 변경)
    StructField("username", StringType(), True),            # 유저 이름
    StructField("chatting_content", StringType(), True),    # 채팅 내용
    StructField("chat_ip", StringType(), True),             # 해당 IP
    StructField("chat_check", StringType(), True)           # 체크 여부
])

log_load = spark.read.csv(log_file, header=False, schema=schema)

# DB에 로그 데이터 삽입 함수
def insert(log_df):
    conn = get_connection()  # 데이터베이스 연결
    with conn:
        with conn.cursor() as cursor:
            # DataFrame의 각 행을 가져와서 파라미터 리스트 생성
            params = log_df.rdd.map(lambda row: (
                row.offset,
                row.chat_time,
                row.username,
                row.chatting_content,
                row.chat_ip,
                row.chat_check
            )).collect()  # collect()를 사용하여 리스트로 변환

            # 중복을 피하기 위해 데이터베이스에서 현재 오프셋 목록을 가져옴
            cursor.execute("SELECT `offset` FROM chatting_supervisor")
            existing_offsets = {row['offset'] for row in cursor.fetchall()}

            # 중복되지 않은 파라미터 필터링
            unique_params = [param for param in params if param[0] not in existing_offsets]

            # INSERT 쿼리 작성
            if unique_params:  # 중복되지 않은 데이터가 있는 경우에만 삽입
                cursor.executemany("""
                    INSERT INTO chatting_supervisor (`offset`, chat_time, username, chatting_content, chat_ip, chat_check)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, unique_params)

            # 변경 사항을 커밋
            conn.commit()


# 로그 데이터 삽입
insert(log_load)

OFFSET_FILE = 'consumer_offset.txt'

def save_offset(offset):
    with open(OFFSET_FILE, 'w') as file:
        file.write(str(offset))

def read_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, 'r') as file:
            return int(file.read().strip())
    return None
