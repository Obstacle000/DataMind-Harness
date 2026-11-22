"""
PostgreSQL 企业数仓客户端 (OLTP / 源业务库)

核心职责：
1. 从 information_schema 提取表结构、主外键血缘约束（支撑 Schema-Map PageRank 与最短 JOIN 路径计算）
2. 执行 EXPLAIN (FORMAT JSON) 成本预估（支撑 SqlSafetyGateMiddleware 拦截全表扫描慢查询）
3. 执行只读分析 SQL 与受控写回 SQL（写回必须经过 HITL 人工审批）
4. 当本地未启动 PostgreSQL 实例时，自动退化为内置的企业数仓演示元数据，确保框架开箱即跑
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from ..config import POSTGRES_URI, POSTGRES_MAX_SCAN_ROWS
from ..log_utils import agent_logger


# 内置的企业级数仓标准 Schema 元数据（当本地未启动 PostgreSQL 容器时自动兜底，支撑 Table-Rank 图谱计算）
DEFAULT_WAREHOUSE_SCHEMA: Dict[str, Dict[str, Any]] = {
    "dim_customers": {
        "description": "客户维度表（城市、区域、客户等级、注册渠道）",
        "primary_key": "customer_id",
        "columns": [
            {"name": "customer_id", "type": "BIGINT", "comment": "客户唯一主键"},
            {"name": "customer_name", "type": "VARCHAR(128)", "comment": "客户/企业名称"},
            {"name": "region", "type": "VARCHAR(32)", "comment": "所属大区（华东/华南/华北/西南）"},
            {"name": "city", "type": "VARCHAR(64)", "comment": "城市名称"},
            {"name": "vip_tier", "type": "VARCHAR(16)", "comment": "客户等级 (S/A/B/C)"},
            {"name": "phone_masked", "type": "VARCHAR(32)", "comment": "脱敏手机号 (PII敏感字段)"},
        ],
        "foreign_keys": [],
    },
    "dim_suppliers": {
        "description": "供应商维度表（评级、交期承诺、合格率）",
        "primary_key": "supplier_id",
        "columns": [
            {"name": "supplier_id", "type": "BIGINT", "comment": "供应商主键"},
            {"name": "supplier_name", "type": "VARCHAR(128)", "comment": "供应商名称"},
            {"name": "region", "type": "VARCHAR(32)", "comment": "所在区域"},
            {"name": "credit_score", "type": "NUMERIC(5,2)", "comment": "信用评分 (0-100)"},
            {"name": "lead_time_days", "type": "INT", "comment": "标准交期天数"},
            {"name": "status", "type": "VARCHAR(16)", "comment": "合作状态 (active/suspended)"},
        ],
        "foreign_keys": [],
    },
    "dim_products": {
        "description": "商品与零部件维度表（品类、标准成本、采购价、安全库存）",
        "primary_key": "product_id",
        "columns": [
            {"name": "product_id", "type": "BIGINT", "comment": "商品/零件主键"},
            {"name": "sku_code", "type": "VARCHAR(64)", "comment": "SKU 编码"},
            {"name": "product_name", "type": "VARCHAR(128)", "comment": "商品名称"},
            {"name": "category", "type": "VARCHAR(64)", "comment": "所属品类"},
            {"name": "supplier_id", "type": "BIGINT", "comment": "主供应商ID"},
            {"name": "unit_cost", "type": "NUMERIC(12,2)", "comment": "标准单位成本"},
            {"name": "safety_stock", "type": "INT", "comment": "安全库存水位"},
        ],
        "foreign_keys": [
            {"column": "supplier_id", "ref_table": "dim_suppliers", "ref_column": "supplier_id"}
        ],
    },
    "fact_orders": {
        "description": "销售与采购主订单事实表（核心枢纽表：订单状态、总金额、运费、折扣）",
        "primary_key": "order_id",
        "columns": [
            {"name": "order_id", "type": "BIGINT", "comment": "订单唯一主键"},
            {"name": "order_no", "type": "VARCHAR(64)", "comment": "业务订单编号"},
            {"name": "customer_id", "type": "BIGINT", "comment": "关联客户ID"},
            {"name": "order_date", "type": "DATE", "comment": "下单日期"},
            {"name": "gross_amount", "type": "NUMERIC(14,2)", "comment": "订单毛销售额"},
            {"name": "discount_amount", "type": "NUMERIC(14,2)", "comment": "优惠券与折扣金额"},
            {"name": "shipping_fee", "type": "NUMERIC(12,2)", "comment": "物流运费"},
            {"name": "order_status", "type": "VARCHAR(32)", "comment": "状态 (pending/paid/shipped/review_hold)"},
        ],
        "foreign_keys": [
            {"column": "customer_id", "ref_table": "dim_customers", "ref_column": "customer_id"}
        ],
    },
    "fact_order_items": {
        "description": "订单明细行事实表（单品销量、成交单价、毛利额）",
        "primary_key": "item_id",
        "columns": [
            {"name": "item_id", "type": "BIGINT", "comment": "明细行主键"},
            {"name": "order_id", "type": "BIGINT", "comment": "关联主订单ID"},
            {"name": "product_id", "type": "BIGINT", "comment": "关联商品ID"},
            {"name": "quantity", "type": "INT", "comment": "购买数量"},
            {"name": "unit_price", "type": "NUMERIC(12,2)", "comment": "实际成交单价"},
            {"name": "line_profit", "type": "NUMERIC(14,2)", "comment": "明细毛利额"},
        ],
        "foreign_keys": [
            {"column": "order_id", "ref_table": "fact_orders", "ref_column": "order_id"},
            {"column": "product_id", "ref_table": "dim_products", "ref_column": "product_id"},
        ],
    },
    "fact_returns": {
        "description": "退货与售后事实表（退货金额、质量缺陷原因、责任供应商）",
        "primary_key": "return_id",
        "columns": [
            {"name": "return_id", "type": "BIGINT", "comment": "退货单主键"},
            {"name": "order_id", "type": "BIGINT", "comment": "关联原订单ID"},
            {"name": "product_id", "type": "BIGINT", "comment": "退货商品ID"},
            {"name": "return_date", "type": "DATE", "comment": "退货日期"},
            {"name": "refund_amount", "type": "NUMERIC(14,2)", "comment": "实际退款金额"},
            {"name": "defect_reason", "type": "VARCHAR(128)", "comment": "退货原因（质量瑕疵/交期延误/包装破损）"},
        ],
        "foreign_keys": [
            {"column": "order_id", "ref_table": "fact_orders", "ref_column": "order_id"},
            {"column": "product_id", "ref_table": "dim_products", "ref_column": "product_id"},
        ],
    },
    "fact_inventory_snapshots": {
        "description": "每日库存快照与在途库存事实表",
        "primary_key": "snapshot_id",
        "columns": [
            {"name": "snapshot_id", "type": "BIGINT", "comment": "快照主键"},
            {"name": "product_id", "type": "BIGINT", "comment": "关联商品ID"},
            {"name": "warehouse_region", "type": "VARCHAR(32)", "comment": "仓库所在大区"},
            {"name": "on_hand_qty", "type": "INT", "comment": "现有可用库存"},
            {"name": "in_transit_qty", "type": "INT", "comment": "在途未入库数量"},
            {"name": "snapshot_date", "type": "DATE", "comment": "快照日期"},
        ],
        "foreign_keys": [
            {"column": "product_id", "ref_table": "dim_products", "ref_column": "product_id"},
        ],
    },
    "fact_quality_claims": {
        "description": "供应商质量索赔孤岛表（无物理外键、无历史SQL、字段异名：ord_ref_no / vendor_code）",
        "primary_key": "claim_id",
        "columns": [
            {"name": "claim_id", "type": "BIGINT", "comment": "索赔单唯一主键"},
            {"name": "ord_ref_no", "type": "BIGINT", "comment": "关联业务订单号（异名外键，未建物理约束）"},
            {"name": "vendor_code", "type": "BIGINT", "comment": "责任供应商编码（异名外键，未建物理约束）"},
            {"name": "claim_region", "type": "VARCHAR(32)", "comment": "发生大区（低基数维度列）"},
            {"name": "claim_amount", "type": "NUMERIC(14,2)", "comment": "质量索赔扣款金额"},
            {"name": "claim_status", "type": "VARCHAR(32)", "comment": "索赔状态"},
        ],
        "foreign_keys": [],  # 无物理外键，由第一阶段第 3 层（数据值包含率 + 大模型语义盖章）自动探查发现！
    },
}


class PostgresClient:
    """PostgreSQL 业务数仓客户端（支持连接池、Schema 自省、EXPLAIN 成本审计与只读查询）"""

    def __init__(self, uri: str = POSTGRES_URI):
        self.uri = uri
        self._driver_available = False
        self._conn = None
        self._try_load_driver()

    def _try_load_driver(self) -> None:
        for mod_name in ("psycopg2", "psycopg"):
            try:
                __import__(mod_name)
                self._driver_available = True
                return
            except ImportError:
                continue

    def _get_connection(self):
        """惰性建立 PostgreSQL 连接；如果本地未启动服务则返回 None"""
        if not self._driver_available:
            return None
        try:
            import psycopg2
            conn = psycopg2.connect(self.uri, connect_timeout=3)
            return conn
        except Exception:
            try:
                import psycopg
                conn = psycopg.connect(self.uri, connect_timeout=3)
                return conn
            except Exception as exc:
                agent_logger.debug(f"PostgreSQL live connection unavailable ({exc}), using mock/DuckDB fallback")
                return None

    def fetch_schema_metadata(self) -> Dict[str, Dict[str, Any]]:
        """获取全库表结构与主外键血缘关系，用于构建 Schema-Map (Table-Rank)"""
        conn = self._get_connection()
        if conn is None:
            return dict(DEFAULT_WAREHOUSE_SCHEMA)

        try:
            schema_map: Dict[str, Dict[str, Any]] = {}
            with conn.cursor() as cur:
                # 1. 查询所有 public 下的表和字段
                cur.execute(
                    """
                    SELECT table_name, column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    ORDER BY table_name, ordinal_position
                    """
                )
                for table_name, col_name, data_type in cur.fetchall():
                    t_entry = schema_map.setdefault(
                        table_name,
                        {
                            "description": f"业务表 {table_name}",
                            "primary_key": "id",
                            "columns": [],
                            "foreign_keys": [],
                        },
                    )
                    t_entry["columns"].append(
                        {"name": col_name, "type": str(data_type).upper(), "comment": ""}
                    )

                # 2. 查询外键关系 (Foreign Keys)
                cur.execute(
                    """
                    SELECT
                        tc.table_name,
                        kcu.column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                      AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                      AND tc.table_schema = 'public'
                    """
                )
                for t_name, col_name, ref_table, ref_col in cur.fetchall():
                    if t_name in schema_map:
                        schema_map[t_name]["foreign_keys"].append(
                            {"column": col_name, "ref_table": ref_table, "ref_column": ref_col}
                        )
            conn.close()
            return schema_map or dict(DEFAULT_WAREHOUSE_SCHEMA)
        except Exception as exc:
            agent_logger.warning(f"Failed to introspect PostgreSQL schema ({exc}), fallback to default warehouse schema")
            try:
                conn.close()
            except Exception:
                pass
            return dict(DEFAULT_WAREHOUSE_SCHEMA)

    def explain_query_cost(self, sql: str) -> Dict[str, Any]:
        """执行 EXPLAIN (FORMAT JSON) 预估查询扫描行数与成本"""
        conn = self._get_connection()
        if conn is None:
            return {"estimated_rows": 500, "total_cost": 12.5, "exceeds_limit": False, "mode": "fallback"}

        try:
            with conn.cursor() as cur:
                cur.execute(f"EXPLAIN (FORMAT JSON) {sql}")
                row = cur.fetchone()
                plan_data = row[0] if row else []
                if isinstance(plan_data, str):
                    plan_data = json.loads(plan_data)
                root_plan = plan_data[0].get("Plan", {}) if plan_data else {}
                plan_rows = int(root_plan.get("Plan Rows", 0))
                total_cost = float(root_plan.get("Total Cost", 0.0))
            conn.close()
            return {
                "estimated_rows": plan_rows,
                "total_cost": total_cost,
                "exceeds_limit": plan_rows > POSTGRES_MAX_SCAN_ROWS,
                "max_allowed_rows": POSTGRES_MAX_SCAN_ROWS,
                "mode": "live_postgres",
            }
        except Exception as exc:
            try:
                conn.close()
            except Exception:
                pass
            return {"estimated_rows": 0, "total_cost": 0.0, "exceeds_limit": False, "error": str(exc)}

    def execute_readonly_query(self, sql: str, limit: int = 200) -> Dict[str, Any]:
        """在只读事务中执行 SQL 查询；若本地未起 PG 则自动转发给 DuckDB 内置演示数仓执行"""
        conn = self._get_connection()
        if conn is None:
            from .duckdb_client import duckdb_client
            return duckdb_client.query_olap(sql, limit=limit)

        try:
            with conn.cursor() as cur:
                cur.execute("SET TRANSACTION READ ONLY;")
                cur.execute(sql)
                columns = [desc[0] for desc in cur.description] if cur.description else []
                rows = cur.fetchmany(limit)
                records = [dict(zip(columns, r)) for r in rows]
            conn.close()
            return {
                "engine": "postgresql",
                "columns": columns,
                "row_count": len(records),
                "rows": records,
            }
        except Exception as exc:
            try:
                conn.close()
            except Exception:
                pass
            return {"engine": "postgresql", "error": str(exc), "rows": []}


# 全局单例
postgres_client = PostgresClient()
