import os

import pymysql
from dotenv import load_dotenv


def get_mysql_config() -> dict:
	load_dotenv()

	def require_env(name: str) -> str:
		value = os.getenv(name)
		if not value:
			raise ValueError(f"Missing required env var: {name}")
		return value

	return {
		"host": require_env("MYSQL_HOST"),
		"port": int(require_env("MYSQL_PORT")),
		"user": require_env("MYSQL_USER"),
		"password": require_env("MYSQL_PASSWORD"),
		"database": require_env("MYSQL_DATABASE"),
		"charset": require_env("MYSQL_CHARSET") if os.getenv("MYSQL_CHARSET") else "utf8mb4",
		"autocommit": True,
	}


def get_mysql_config_without_db() -> dict:
	"""获取不指定数据库的配置（用于创建数据库）"""
	load_dotenv()

	def require_env(name: str) -> str:
		value = os.getenv(name)
		if not value:
			raise ValueError(f"Missing required env var: {name}")
		return value

	return {
		"host": require_env("MYSQL_HOST"),
		"port": int(require_env("MYSQL_PORT")),
		"user": require_env("MYSQL_USER"),
		"password": require_env("MYSQL_PASSWORD"),
		"charset": require_env("MYSQL_CHARSET") if os.getenv("MYSQL_CHARSET") else "utf8mb4",
		"autocommit": True,
	}


def create_database_if_not_exists():
	"""自动创建数据库（如果不存在）"""
	config = get_mysql_config_without_db()
	database_name = os.getenv("MYSQL_DATABASE")

	connection = pymysql.connect(**config)
	try:
		with connection.cursor() as cursor:
			cursor.execute(
				f"CREATE DATABASE IF NOT EXISTS {database_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
			print(f"数据库 '{database_name}' 检查/创建完成")
	finally:
		connection.close()


TABLE_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS t_prediction_result (
        predict_id        BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '预测ID',
        biz_date          DATE NOT NULL COMMENT '预测日期',
        category_code     VARCHAR(50) NOT NULL COMMENT '品类代码',
        predicted_value   DECIMAL(18, 2) NOT NULL COMMENT '预测配运量',
        rec_interval_start DATETIME COMMENT '推荐收货开始时间',
        rec_interval_end   DATETIME COMMENT '推荐收货结束时间',
        status            TINYINT DEFAULT 1 COMMENT '状态: 1-有效, 0-已覆盖/作废',
        adjust_reason     VARCHAR(255) COMMENT '人工调整原因',
        adjusted_value    DECIMAL(18, 2) COMMENT '调整后数值(若为空则为原始预测)',
        operator          VARCHAR(50) COMMENT '最后操作人',
        create_time       DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        update_time       DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        INDEX idx_date_category (biz_date, category_code),
        INDEX idx_status (status)
    ) COMMENT='预测结果表';
    """,
    """
    CREATE TABLE IF NOT EXISTS t_price_alert (
        alert_id          BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '预警ID',
        rule_id           VARCHAR(50) NOT NULL COMMENT '触发规则ID',
        rule_name         VARCHAR(100) COMMENT '规则名称快照',
        category_code     VARCHAR(50) NOT NULL COMMENT '关联品类',
        current_price     DECIMAL(18, 4) NOT NULL COMMENT '触发时的当前价格',
        threshold_value   DECIMAL(18, 4) COMMENT '触发阈值',
        trigger_reason    VARCHAR(255) COMMENT '触发具体原因描述',
        level             TINYINT COMMENT '预警级别: 1-低, 2-中, 3-高',
        status            TINYINT DEFAULT 0 COMMENT '状态: 0-未确认, 1-已确认, 2-已关闭',
        confirm_user      VARCHAR(50) COMMENT '确认人',
        confirm_time      DATETIME COMMENT '确认时间',
        create_time       DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '触发时间',
        INDEX idx_category_status (category_code, status),
        INDEX idx_create_time (create_time)
    ) COMMENT='价格预警记录表';
    """,
    """
    CREATE TABLE IF NOT EXISTS t_supply_anomaly (
        anomaly_id        BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '异常ID',
        category_code     VARCHAR(50) NOT NULL COMMENT '受影响品类',
        supplier_code     VARCHAR(50) NOT NULL COMMENT '供应商代码',
        supplier_name     VARCHAR(100) COMMENT '供应商名称',
        anomaly_type      VARCHAR(50) COMMENT '异常类型: 断供/延迟/质量等',
        description       TEXT COMMENT '异常详细描述',
        impact_scope      VARCHAR(255) COMMENT '影响范围描述',
        duration_days     INT COMMENT '预计持续天数',
        status            TINYINT DEFAULT 0 COMMENT '处理状态: 0-待处理, 1-处理中, 2-已解决',
        recommended_actions JSON COMMENT '推荐应对动作列表(JSON格式)',
        handler           VARCHAR(50) COMMENT '当前处理人',
        create_time       DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '发现时间',
        resolve_time      DATETIME COMMENT '解决时间',
        INDEX idx_supplier_status (supplier_code, status),
        INDEX idx_category (category_code)
    ) COMMENT='供应异常记录表';
    """,
    """
    CREATE TABLE IF NOT EXISTS t_allocation_plan (
        plan_id           BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '方案ID',
        biz_date          DATE NOT NULL COMMENT '执行日期',
        category_range    VARCHAR(255) COMMENT '适用品类范围',
        plan_details      JSON NOT NULL COMMENT '方案详情: 包含各仓库分配量、物流指令等(JSON)',
        input_factors     JSON COMMENT '输入因素快照: 库存/合同/成本等',
        expected_kpi      JSON COMMENT '预期KPI评估: 成本/时效/满意度',
        status            TINYINT DEFAULT 0 COMMENT '状态: 0-草稿, 1-已发布, 2-执行中, 3-已完成, 4-已取消',
        creator           VARCHAR(50) COMMENT '创建人/生成算法版本',
        create_time       DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '生成时间',
        execute_log       TEXT COMMENT '执行日志摘要',
        INDEX idx_date_status (biz_date, status),
        INDEX idx_creator (creator)
    ) COMMENT='动态分配方案表';
    """
]

def create_tables() -> None:
	# 第1步：先创建数据库（如果不存在）
	create_database_if_not_exists()

	# 第2步：创建表
	config = get_mysql_config()
	connection = pymysql.connect(**config)
	try:
		with connection.cursor() as cursor:
			for statement in TABLE_STATEMENTS:
				cursor.execute(statement)
		print("所有数据表创建完成")
	finally:
		connection.close()


if __name__ == "__main__":
	create_tables()