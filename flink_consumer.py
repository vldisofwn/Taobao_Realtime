from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Time
from pyflink.datastream.window import TumblingEventTimeWindows
import json
import logging
import threading
import queue
import time
import os
from collections import defaultdict, Counter
from datetime import datetime
from db import get_connection

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 创建数据队列
flink_data_queue = queue.Queue()


class EventTimeExtractor(MapFunction):
    """提取事件时间并添加时间戳"""

    def map(self, value):
        try:
            data = json.loads(value)

            # 从数据中提取时间信息，构造事件时间
            if 'month' in data and 'day' in data:
                # 构造当年的时间戳
                current_year = datetime.now().year
                try:
                    event_time = datetime(current_year, int(data['month']), int(data['day']))
                    event_timestamp = int(event_time.timestamp() * 1000)
                except:
                    # 如果日期无效，使用当前时间
                    event_timestamp = int(time.time() * 1000)
            else:
                # 如果没有时间字段，使用当前时间
                event_timestamp = int(time.time() * 1000)

            # 添加时间戳信息
            data['event_timestamp'] = event_timestamp
            data['processing_timestamp'] = int(time.time() * 1000)

            return json.dumps(data)

        except Exception as e:
            logger.error(f"提取事件时间失败: {str(e)}")
            # 返回原始数据，添加当前时间戳
            try:
                data = json.loads(value)
                data['event_timestamp'] = int(time.time() * 1000)
                data['processing_timestamp'] = int(time.time() * 1000)
                return json.dumps(data)
            except:
                return value


class WindowedDataProcessor(MapFunction):
    """基于窗口的数据处理器"""

    def __init__(self):
        self.late_data_count = 0
        self.processed_count = 0

    def map(self, value):
        try:
            data = json.loads(value)
            self.processed_count += 1

            # 检查是否是迟到数据
            current_time = int(time.time() * 1000)
            event_time = data.get('event_timestamp', current_time)

            # 计算延迟时间
            delay_seconds = (current_time - event_time) / 1000

            # 如果事件时间比当前时间早5分钟以上，认为是迟到数据
            if delay_seconds > 300:  # 5分钟 = 300秒
                self.late_data_count += 1
                logger.warning(f"检测到迟到数据 #{self.late_data_count}, 延迟: {delay_seconds:.2f}秒")
                # 标记为迟到数据但仍然处理
                data['is_late'] = True
                data['delay_seconds'] = delay_seconds
                return json.dumps(data)

            # 正常数据处理
            data['is_late'] = False
            data['delay_seconds'] = delay_seconds

            if self.processed_count % 100 == 0:  # 每100条记录输出一次统计
                logger.info(f"已处理 {self.processed_count} 条记录，其中迟到数据 {self.late_data_count} 条")

            return json.dumps(data)

        except Exception as e:
            logger.error(f"窗口数据处理错误: {str(e)}")
            return value


def create_enhanced_flink_environment():
    """创建支持事件时间的Flink环境"""
    try:
        logger.info("创建Flink环境...")

        # 创建流处理环境
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)

        # 添加Kafka连接器JAR包
        jar_path = "/opt/module/code/Taobao_Realtime/flink_lib/flink-sql-connector-kafka-1.17.1.jar"
        abs_jar_path = os.path.abspath(jar_path)

        if os.path.exists(abs_jar_path):
            logger.info(f"添加JAR包: {abs_jar_path}")
            env.add_jars(f"file://{abs_jar_path}")
        else:
            logger.warning(f"JAR包不存在: {abs_jar_path}")

        # 启用checkpoint
        env.enable_checkpointing(5000)

        logger.info("Flink环境创建成功")
        return env

    except Exception as e:
        logger.error(f"创建Flink环境失败: {str(e)}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        raise


def setup_enhanced_kafka_source(env):
    """设置Kafka数据源"""
    try:
        logger.info("设置Kafka数据源...")

        # Kafka配置
        kafka_props = {
            'bootstrap.servers': 'hadoop105:9092',
            'group.id': 'flink-enhanced-group',
            'auto.offset.reset': 'latest'
        }

        # 创建Kafka消费者
        kafka_consumer = FlinkKafkaConsumer(
            topics='taobao_data2',
            deserialization_schema=SimpleStringSchema(),
            properties=kafka_props
        )

        # 从最新位置开始读取
        kafka_consumer.set_start_from_latest()

        logger.info("Kafka源配置成功")
        return kafka_consumer

    except Exception as e:
        logger.error(f"设置Kafka源失败: {str(e)}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        raise


class EnhancedDataProcessor(MapFunction):
    """数据处理器"""

    def __init__(self):
        self.batch_data = []
        self.batch_size = 10
        self.last_process_time = time.time()
        self.message_count = 0
        self.late_message_count = 0
        self.total_delay = 0

    def map(self, value):
        try:
            self.message_count += 1
            data = json.loads(value)

            # 记录延迟统计
            if 'delay_seconds' in data:
                self.total_delay += data['delay_seconds']
                avg_delay = self.total_delay / self.message_count

                if data.get('is_late', False):
                    self.late_message_count += 1

                if self.message_count % 50 == 0:  # 每50条消息输出一次统计
                    logger.info(
                        f"处理统计 - 总计: {self.message_count}, 迟到: {self.late_message_count}, 平均延迟: {avg_delay:.2f}秒")

            # 验证数据格式
            if self.validate_data(data):
                self.batch_data.append(data)

                # 批处理逻辑
                current_time = time.time()
                if len(self.batch_data) >= self.batch_size or (current_time - self.last_process_time) > 2:
                    self.process_batch()
                    self.last_process_time = current_time

            return value

        except Exception as e:
            logger.error(f"处理消息时出错: {str(e)}")
            return value

    def validate_data(self, data):
        """验证数据格式"""
        required_fields = ['action', 'age_range', 'gender', 'province']
        for field in required_fields:
            if field not in data or data[field] is None:
                return False
        return True

    def process_batch(self):
        """处理批次数据"""
        if not self.batch_data:
            return

        try:
            # 分离正常数据和迟到数据
            normal_data = [item for item in self.batch_data if not item.get('is_late', False)]
            late_data = [item for item in self.batch_data if item.get('is_late', False)]

            logger.info(f"处理批次 - 总数: {len(self.batch_data)}, 正常: {len(normal_data)}, 迟到: {len(late_data)}")

            # 统计各维度数据（只统计正常数据）
            action_stats = Counter(str(item['action']) for item in normal_data)
            age_stats = Counter(str(item['age_range']) for item in normal_data)
            gender_stats = Counter(str(item['gender']) for item in normal_data)
            province_stats = Counter(item['province'] for item in normal_data)

            # 添加时间和延迟统计信息
            current_window = int(time.time() / 10) * 10  # 10秒窗口对齐
            avg_delay = sum(item.get('delay_seconds', 0) for item in normal_data) / max(len(normal_data), 1)

            data = {
                'action': dict(action_stats),
                'age': dict(age_stats),
                'gender': dict(gender_stats),
                'province': dict(province_stats),
                'window_time': current_window,
                'batch_size': len(normal_data),
                'late_data_count': len(late_data),
                'avg_delay_seconds': round(avg_delay, 2),
                'timestamp': datetime.now().isoformat()
            }

            # 将数据放入队列
            flink_data_queue.put(data)
            logger.info(
                f"增强批次处理完成 - 窗口: {datetime.fromtimestamp(current_window)}, 平均延迟: {avg_delay:.2f}秒")

            # 清空批次数据
            self.batch_data = []
            self.save_to_database(data)

        except Exception as e:
            logger.error(f"Error processing enhanced batch: {str(e)}")

    def save_to_database(self, data):
        try:
            conn = get_connection()
            if conn:
                with conn.cursor() as cursor:
                    sql = """
                        INSERT INTO user_stats (window_time, action_stats, age_stats, gender_stats, province_stats, avg_delay)
                        VALUES (FROM_UNIXTIME(%s), %s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql, (
                        data['window_time'],
                        json.dumps(data['action'], ensure_ascii=False),
                        json.dumps(data['age'], ensure_ascii=False),
                        json.dumps(data['gender'], ensure_ascii=False),
                        json.dumps(data['province'], ensure_ascii=False),
                        data['avg_delay_seconds']
                    ))
                    conn.commit()
                    logger.info("Data inserted into database successfully.")
                conn.close()
        except Exception as e:
            logger.error(f"Failed to insert data into database: {str(e)}")


def start_enhanced_flink_consumer():
    """启动Flink消费者"""
    try:
        logger.info("=== 启动Flink消费者 ===")

        # 创建Flink环境
        logger.info("1. 创建Flink环境...")
        env = create_enhanced_flink_environment()

        # 设置Kafka数据源
        logger.info("2. 设置Kafka数据源...")
        kafka_source = setup_enhanced_kafka_source(env)

        # 添加数据源到流
        logger.info("3. 添加数据源...")
        kafka_stream = env.add_source(kafka_source)

        # 提取事件时间并添加时间戳
        logger.info("4. 设置事件时间提取...")
        timestamped_stream = kafka_stream.map(EventTimeExtractor(), output_type=Types.STRING())

        # 窗口数据处理（检测迟到数据）
        logger.info("5. 设置窗口和迟到数据检测...")
        windowed_stream = timestamped_stream.map(WindowedDataProcessor(), output_type=Types.STRING())

        # 最终数据处理
        logger.info("6. 设置最终数据处理...")
        processed_stream = windowed_stream.map(EnhancedDataProcessor(), output_type=Types.STRING())

        # 启动作业
        logger.info("7. 启动Flink作业...")
        env.execute("Enhanced Taobao Flink Consumer")

    except Exception as e:
        logger.error(f"Flink消费者启动失败: {str(e)}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")


def start_flink_consumer_thread():
    """启动Flink消费者线程"""
    try:
        logger.info("启动Flink消费者线程...")
        consumer_thread = threading.Thread(target=start_enhanced_flink_consumer, daemon=True)
        consumer_thread.start()
        logger.info("Flink consumer thread started successfully")
        return consumer_thread
    except Exception as e:
        logger.error(f"Failed to start enhanced Flink consumer thread: {str(e)}")
        return None


if __name__ == "__main__":
    start_enhanced_flink_consumer()