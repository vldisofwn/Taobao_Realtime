import json
import time
import random
import pandas as pd
from kafka import KafkaProducer
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['hadoop105:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            security_protocol="PLAINTEXT",
            api_version_auto_timeout_ms=30000,
            request_timeout_ms=30000
        )
        logger.info("Kafka producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {str(e)}")
        raise

def generate_data(csv_file):
    try:
        df = pd.read_csv(csv_file)
        logger.info(f"Successfully loaded CSV file: {csv_file}")
        return df
    except Exception as e:
        logger.error(f"Failed to read CSV file: {str(e)}")
        raise

def main():
    try:
        # 创建生产者
        producer = create_producer()
        
        # 读取CSV文件
        df = generate_data('111.csv')
        
        # 设置topic
        topic = "taobao_data2"
        
        # 发送数据
        for _, row in df.iterrows():
            try:
                data = row.to_dict()
                producer.send(topic, value=data)
                logger.info(f"Sent data to topic {topic}: {data}")
                time.sleep(0.1)  # 控制发送速率
            except Exception as e:
                logger.error(f"Failed to send data: {str(e)}")
                continue
        
        # 确保所有消息都被发送
        producer.flush()
        logger.info("All data has been sent successfully")
        
    except Exception as e:
        logger.error(f"An error occurred in main: {str(e)}")
    finally:
        if 'producer' in locals():
            producer.close()
            logger.info("Producer closed")

if __name__ == "__main__":
    main() 
