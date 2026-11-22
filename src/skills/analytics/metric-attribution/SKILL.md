---
name: metric-attribution
description: 经营指标异动归因分析与异常下钻标准作业程序 (SOP)
scope: analytics
version: 1.0.0
---

# 经营指标异动归因分析 SOP (Metric Attribution Playbook)

当用户询问“为什么某区域/品类净利率、销售额或退货率出现大幅波动”时，严格按以下 4 步开展数据科学归因：

## Step 1: 血缘寻路与口径对齐
1. 检查 WARM 记忆中的业务指标口径（默认：`净利 = 毛利(line_profit) - 退款(refund_amount) - 运费(shipping_fee)`）。
2. 调用 `inspect_schema_map` 获取 `fact_orders`、`fact_order_items`、`fact_returns`、`dim_customers`、`dim_products`、`dim_suppliers` 的最短 JOIN 路径。

## Step 2: 维度下钻分解 (Drill-down Decomposition)
调用 `query_duckdb_olap` 或 `execute_readonly_sql`，按以下三个正交维度拆解贡献度：
- **区域/客户维度 (`dim_customers.region`, `city`)**
- **商品/品类维度 (`dim_products.category`, `product_name`)**
- **供应商质量维度 (`dim_suppliers.supplier_name`, `fact_returns.defect_reason`)**

## Step 3: 生成式交互可视化 (Generative UI)
调用 `generate_echarts_schema` 生成动态图表（如瀑布图 `waterfall` 展示毛利到净利的扣减项，或柱状图 `bar` 对比各供应商退货金额）。

## Step 4: 风险写回与闭环建议
若用户要求将问题订单或异常供应商冻结/标记待审，调用 `execute_writeback_sql`（触发 HITL 人工审批）。
