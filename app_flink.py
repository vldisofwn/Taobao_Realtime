from flask import Flask, render_template, request, jsonify, send_file
from flask_socketio import SocketIO
#from flink_consumer import start_flink_consumer_thread, flink_data_queue
from flink_consumer import start_flink_consumer_thread, flink_data_queue
from pyecharts import options as opts
from pyecharts.charts import Bar, Pie
import logging
import time
import json
from db import query_user_stats, export_user_stats_to_csv

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode='threading',  # 改为threading模式
    ping_timeout=60,
    ping_interval=25,
    max_http_buffer_size=1e8
)

# 数据映射字典
ACTION_MAP = {
    '0': '点击',
    '1': '加入购物车',
    '2': '购买',
    '3': '关注商品'
}
AGE_MAP = {
    '1': '年龄<18',
    '2': '18-24岁',
    '3': '25-29岁',
    '4': '30-34岁',
    '5': '35-39岁',
    '6': '40-49岁',
    '7': '年龄>=50',
    '0': '未知'
}
GENDER_MAP = {
    '0': '女性',
    '1': '男性',
    '2': '未知'
}

# 累计统计全局变量
cumulative_stats = {
    'action': {},
    'age': {},
    'gender': {},
    'province': {}
}


def accumulate(target, incoming):
    for k, v in incoming.items():
        target[k] = target.get(k, 0) + v


def create_action_chart(data):
    try:
        if not data:
            return None
        mapped_data = {ACTION_MAP.get(k, k): v for k, v in data.items()}
        pie = (
            Pie(init_opts=opts.InitOpts(theme="dark"))
            .add(
                "",
                [list(z) for z in zip(mapped_data.keys(), mapped_data.values())],
                radius=["40%", "75%"],
            )
            .set_global_opts(
                title_opts=opts.TitleOpts(title="用户行为分布 (Flink)"),
                legend_opts=opts.LegendOpts(orient="vertical", pos_top="15%", pos_left="2%")
            )
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"))
        )
        return json.loads(pie.dump_options())
    except Exception as e:
        logger.error(f"Error creating action chart: {str(e)}")
        return None


def create_age_chart(data):
    try:
        if not data:
            return None
        mapped_data = {AGE_MAP.get(k, k): v for k, v in data.items()}
        bar = (
            Bar(init_opts=opts.InitOpts(theme="dark"))
            .add_xaxis(list(mapped_data.keys()))
            .add_yaxis("", list(mapped_data.values()))
            .set_global_opts(
                title_opts=opts.TitleOpts(title="年龄分布 (Flink)"),
                xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=15)),
                datazoom_opts=[opts.DataZoomOpts()],
            )
        )
        return json.loads(bar.dump_options())
    except Exception as e:
        logger.error(f"Error creating age chart: {str(e)}")
        return None


def create_gender_chart(data):
    try:
        if not data:
            return None
        mapped_data = {GENDER_MAP.get(k, k): v for k, v in data.items()}
        pie = (
            Pie(init_opts=opts.InitOpts(theme="dark"))
            .add(
                "",
                [list(z) for z in zip(mapped_data.keys(), mapped_data.values())],
                radius=["40%", "75%"],
            )
            .set_global_opts(
                title_opts=opts.TitleOpts(title="性别分布 (Flink)"),
                legend_opts=opts.LegendOpts(orient="vertical", pos_top="15%", pos_left="2%")
            )
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"))
        )
        return json.loads(pie.dump_options())
    except Exception as e:
        logger.error(f"Error creating gender chart: {str(e)}")
        return None


def create_province_chart(data):
    try:
        if not data:
            return None
        bar = (
            Bar(init_opts=opts.InitOpts(theme="dark"))
            .add_xaxis(list(data.keys()))
            .add_yaxis("", list(data.values()))
            .set_global_opts(
                title_opts=opts.TitleOpts(title="省份分布 (Flink)"),
                xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=45)),
                datazoom_opts=[opts.DataZoomOpts()],
            )
        )
        return json.loads(bar.dump_options())
    except Exception as e:
        logger.error(f"Error creating province chart: {str(e)}")
        return None


@app.route('/')
def index():
    return render_template('index.html')


@socketio.on('connect')
def handle_connect():
    try:
        logger.info('Client connected')
        empty_data = {'action': {}, 'age': {}, 'gender': {}, 'province': {}}
        socketio.emit('update_data', empty_data)
    except Exception as e:
        logger.error(f"Error in handle_connect: {str(e)}")


@socketio.on('disconnect')
def handle_disconnect():
    try:
        logger.info('Client disconnected')
    except Exception as e:
        logger.error(f"Error in handle_disconnect: {str(e)}")


@socketio.on('request_charts')
def handle_request_charts(data):
    try:
        charts_data = {
            'action': create_action_chart(cumulative_stats['action']),
            'age': create_age_chart(cumulative_stats['age']),
            'gender': create_gender_chart(cumulative_stats['gender']),
            'province': create_province_chart(cumulative_stats['province'])
        }

        has_data = any(cumulative_stats[key] for key in cumulative_stats)
        if has_data:
            socketio.emit('update_charts', charts_data)
            logger.info("Current charts data sent to client")
        else:
            empty_data = {
                'action': create_action_chart({}),
                'age': create_age_chart({}),
                'gender': create_gender_chart({}),
                'province': create_province_chart({})
            }
            socketio.emit('update_charts', empty_data)
            logger.info("Empty charts data sent to client - no data available yet")
    except Exception as e:
        logger.error(f"Error in handle_request_charts: {str(e)}")
        socketio.emit('error', {'message': str(e)})


def background_thread():
    try:
        logger.info("Background thread started")
        while True:
            time.sleep(1)  # 改为使用time.sleep而不是socketio.sleep
            if not flink_data_queue.empty():
                data = flink_data_queue.get()
                logger.info(f"Processing Flink data from queue: {data}")

                accumulate(cumulative_stats['action'], data.get('action', {}))
                accumulate(cumulative_stats['age'], data.get('age', {}))
                accumulate(cumulative_stats['gender'], data.get('gender', {}))
                accumulate(cumulative_stats['province'], data.get('province', {}))

                logger.info(f"Updated cumulative stats: {cumulative_stats}")

                charts_data = {
                    'action': create_action_chart(cumulative_stats['action']),
                    'age': create_age_chart(cumulative_stats['age']),
                    'gender': create_gender_chart(cumulative_stats['gender']),
                    'province': create_province_chart(cumulative_stats['province'])
                }
                socketio.emit('update_charts', charts_data)
                logger.info("Flink charts data updated and sent to all clients")
            else:
                if int(time.time()) % 10 == 0:
                    logger.info(
                        f"Flink queue is empty. Current cumulative stats size: action={len(cumulative_stats['action'])}, age={len(cumulative_stats['age'])}, gender={len(cumulative_stats['gender'])}, province={len(cumulative_stats['province'])}")
    except Exception as e:
        logger.error(f"Error in background_thread: {str(e)}")


@app.route('/api/user_stats')
def api_user_stats():
    try:
        limit = int(request.args.get('limit', 100))
        offset = int(request.args.get('offset', 0))
        data = query_user_stats(limit=limit, offset=offset)
        return jsonify({'code': 0, 'data': data})
    except Exception as e:
        return jsonify({'code': 1, 'msg': str(e)})


@app.route('/api/export_user_stats')
def api_export_user_stats():
    try:
        csv_path = 'user_stats_export.csv'
        success = export_user_stats_to_csv(csv_path)
        if success:
            return send_file(csv_path, as_attachment=True)
        else:
            return jsonify({'code': 1, 'msg': '导出失败'})
    except Exception as e:
        return jsonify({'code': 1, 'msg': str(e)})


if __name__ == '__main__':
    try:
        logger.info("启动Flink消费者线程...")
        start_flink_consumer_thread()

        logger.info("启动后台任务...")
        socketio.start_background_task(background_thread)

        logger.info("Starting Flask application with Flink consumer")
        logger.info("访问 http://hadoop105:5001 查看应用")
        socketio.run(app, host='0.0.0.0', port=5001, debug=False)
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        import traceback

        logger.error(f"详细错误: {traceback.format_exc()}")