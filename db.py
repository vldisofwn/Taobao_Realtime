import pymysql
import logging
from dbutils.pooled_db import PooledDB
import pandas as pd

logger = logging.getLogger(__name__)

# 连接池配置
POOL = PooledDB(
    creator=pymysql,
    maxconnections=5,
    mincached=2,
    maxcached=5,
    blocking=True,
    host='localhost',
    user='root',
    password='xiao159205',
    database='flink_recommend',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

def get_connection():
    try:
        connection = POOL.connection()
        logger.info("Database connection (from pool) established.")
        return connection
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        return None

def query_user_stats(limit=100, offset=0):
    sql = "SELECT * FROM user_stats ORDER BY window_time DESC LIMIT %s OFFSET %s"
    try:
        conn = get_connection()
        if conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (limit, offset))
                result = cursor.fetchall()
            conn.close()
            return result
    except Exception as e:
        logger.error(f"Failed to query user_stats: {str(e)}")
        return []

def export_user_stats_to_csv(csv_path):
    sql = "SELECT * FROM user_stats ORDER BY window_time DESC"
    try:
        conn = get_connection()
        if conn:
            df = pd.read_sql(sql, conn)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            conn.close()
            logger.info(f"Exported user_stats to {csv_path}")
            return True
    except Exception as e:
        logger.error(f"Failed to export user_stats to CSV: {str(e)}")
    return False 