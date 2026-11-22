"""
DuckDB 进程内向量化 OLAP 分析引擎客户端

核心职责：
1. 提供单机极速列存 SQL 查询能力（直接查询 Parquet / CSV / 内存表）
2. 内置一套完整的企业经营分析演示数据集（dim_customers, dim_suppliers, dim_products, fact_orders, fact_order_items, fact_returns, fact_inventory_snapshots）
   确保即使未连接外部 PostgreSQL，Agent 也能立即执行真实的跨表 JOIN、窗口函数与同比/环比归因分析！
3. 当环境中未安装 duckdb 库时，自动使用内置 SQLite 内存引擎作为兼容回退，保证 100% 可执行
"""
from __future__ import annotations

import sqlite3
import threading
from typing import Any, Dict, List

from ..config import DUCKDB_DATABASE_PATH
from ..log_utils import agent_logger


_SEED_SQL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS dim_customers (
        customer_id INTEGER PRIMARY KEY,
        customer_name VARCHAR(128),
        region VARCHAR(32),
        city VARCHAR(64),
        vip_tier VARCHAR(16),
        phone_masked VARCHAR(32)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_suppliers (
        supplier_id INTEGER PRIMARY KEY,
        supplier_name VARCHAR(128),
        region VARCHAR(32),
        credit_score DOUBLE,
        lead_time_days INTEGER,
        status VARCHAR(16)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_products (
        product_id INTEGER PRIMARY KEY,
        sku_code VARCHAR(64),
        product_name VARCHAR(128),
        category VARCHAR(64),
        supplier_id INTEGER,
        unit_cost DOUBLE,
        safety_stock INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_orders (
        order_id INTEGER PRIMARY KEY,
        order_no VARCHAR(64),
        customer_id INTEGER,
        order_date VARCHAR(32),
        gross_amount DOUBLE,
        discount_amount DOUBLE,
        shipping_fee DOUBLE,
        order_status VARCHAR(32)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_order_items (
        item_id INTEGER PRIMARY KEY,
        order_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        unit_price DOUBLE,
        line_profit DOUBLE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_returns (
        return_id INTEGER PRIMARY KEY,
        order_id INTEGER,
        product_id INTEGER,
        return_date VARCHAR(32),
        refund_amount DOUBLE,
        defect_reason VARCHAR(128)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_inventory_snapshots (
        snapshot_id INTEGER PRIMARY KEY,
        product_id INTEGER,
        warehouse_region VARCHAR(32),
        on_hand_qty INTEGER,
        in_transit_qty INTEGER,
        snapshot_date VARCHAR(32)
    );
    """,
    # Seed data
    "DELETE FROM dim_customers;",
    """
    INSERT INTO dim_customers VALUES
    (101, '上海智行工业集团', '华东', '上海', 'S', '138****9001'),
    (102, '杭州云途科技', '华东', '杭州', 'A', '139****2310'),
    (103, '深圳鹏城精密制造', '华南', '深圳', 'S', '135****8821'),
    (104, '苏州恒达动力', '华东', '苏州', 'B', '137****4419'),
    (105, '北京北方重工', '华北', '北京', 'A', '136****1102');
    """,
    "DELETE FROM dim_suppliers;",
    """
    INSERT INTO dim_suppliers VALUES
    (201, '恒力精密轴承厂', '华东', 94.5, 5, 'active'),
    (202, '万向传动齿轮有限公司', '华东', 88.0, 7, 'active'),
    (203, '华南宏达电子元件厂', '华南', 71.5, 14, 'active'),
    (204, '天成液压件制造', '华北', 64.0, 18, 'suspended');
    """,
    "DELETE FROM dim_products;",
    """
    INSERT INTO dim_products VALUES
    (301, 'SKU-BRG-01', '高速精密主轴轴承', '轴承传动', 201, 120.0, 200),
    (302, 'SKU-GER-02', '行星减速齿轮箱', '轴承传动', 202, 450.0, 80),
    (303, 'SKU-ECU-03', '智能电控驱动模块', '电子控制', 203, 680.0, 150),
    (304, 'SKU-HYD-04', '高压液压伺服阀', '液压系统', 204, 890.0, 60);
    """,
    "DELETE FROM fact_orders;",
    """
    INSERT INTO fact_orders VALUES
    (1001, 'ORD-202610-01', 101, '2026-10-05', 68000.0, 2000.0, 1200.0, 'paid'),
    (1002, 'ORD-202610-02', 102, '2026-10-12', 54000.0, 1500.0, 900.0, 'paid'),
    (1003, 'ORD-202610-03', 103, '2026-10-18', 92000.0, 3000.0, 1500.0, 'paid'),
    (1004, 'ORD-202611-01', 101, '2026-11-03', 71000.0, 4500.0, 3800.0, 'paid'),
    (1005, 'ORD-202611-02', 104, '2026-11-09', 62000.0, 3200.0, 2900.0, 'paid'),
    (1006, 'ORD-202611-03', 105, '2026-11-15', 48000.0, 1000.0, 1100.0, 'pending');
    """,
    "DELETE FROM fact_order_items;",
    """
    INSERT INTO fact_order_items VALUES
    (5001, 1001, 301, 200, 180.0, 12000.0),
    (5002, 1001, 303, 40, 800.0, 4800.0),
    (5003, 1002, 302, 100, 540.0, 9000.0),
    (5004, 1003, 301, 300, 180.0, 18000.0),
    (5005, 1004, 303, 80, 750.0, 5600.0),
    (5006, 1005, 304, 60, 950.0, 3600.0),
    (5007, 1006, 302, 80, 600.0, 12000.0);
    """,
    "DELETE FROM fact_returns;",
    """
    INSERT INTO fact_returns VALUES
    (9001, 1001, 303, '2026-10-20', 3200.0, '驱动模块温控批次瑕疵'),
    (9002, 1004, 303, '2026-11-10', 15000.0, '智能电控驱动模块电压不稳返修'),
    (9003, 1005, 304, '2026-11-14', 11400.0, '液压伺服阀密封圈漏油');
    """,
    "DELETE FROM fact_inventory_snapshots;",
    """
    INSERT INTO fact_inventory_snapshots VALUES
    (7001, 301, '华东', 320, 100, '2026-11-20'),
    (7002, 302, '华东', 55, 20, '2026-11-20'),
    (7003, 303, '华南', 90, 0, '2026-11-20'),
    (7004, 304, '华北', 25, 0, '2026-11-20');
    """,
]


class DuckDBClient:
    """进程内向量化 OLAP 客户端（优先使用 DuckDB，若未安装则无缝回退到内存 SQLite）"""

    def __init__(self, database_path: str = DUCKDB_DATABASE_PATH):
        self.database_path = database_path
        self._lock = threading.RLock()
        self._engine_name = "duckdb"
        self._conn = None
        self._init_engine()

    def _init_engine(self) -> None:
        try:
            import duckdb
            self._conn = duckdb.connect(self.database_path)
            self._engine_name = "duckdb"
            agent_logger.info(f"DuckDB OLAP engine initialized ({self.database_path})")
        except ImportError:
            self._conn = sqlite3.connect(":memory:", check_same_thread=False)
            self._engine_name = "duckdb_sqlite_compat"
            agent_logger.info("duckdb package not installed, using in-memory SQLite OLAP compatibility engine")

        with self._lock:
            for stmt in _SEED_SQL_STATEMENTS:
                try:
                    self._conn.execute(stmt)
                except Exception as exc:
                    agent_logger.debug(f"Seed statement warning: {exc}")
            if self._engine_name != "duckdb":
                self._conn.commit()

    def query_olap(self, sql: str, limit: int = 500) -> Dict[str, Any]:
        """执行向量化 OLAP 查询并返回列名与字典行列表"""
        with self._lock:
            try:
                cur = self._conn.execute(sql)
                columns = [desc[0] for desc in cur.description] if cur.description else []
                raw_rows = cur.fetchmany(limit)
                rows = [dict(zip(columns, r)) for r in raw_rows]
                return {
                    "engine": self._engine_name,
                    "columns": columns,
                    "row_count": len(rows),
                    "rows": rows,
                }
            except Exception as exc:
                return {
                    "engine": self._engine_name,
                    "error": str(exc),
                    "rows": [],
                }


# 全局单例
duckdb_client = DuckDBClient()
