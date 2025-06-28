import streamlit as st
import pandas as pd
import asyncio
import traceback
from datetime import datetime
from hk_frozen_food_spider import crawl_target, TARGETS, SafeRedisSet, r

st.set_page_config(page_title="港澳冷冻食品供应商爬虫（AI增强版）", layout="wide")
st.title("港澳冷冻食品供应商爬虫（AI增强版）")

# 采集参数
selected_target = st.selectbox(
    "选择采集目标", [t['name'] for t in TARGETS] + ["全部"]
)
concurrency = st.slider("并发数", min_value=1, max_value=20, value=10)

if st.button("开始采集"):
    st.info("采集已启动，请稍候...（如目标为动态页面，采集时间可能较长）")
    results = []
    safe_set = SafeRedisSet('company_names', r)
    progress = st.progress(0)
    log_area = st.empty()
    async def run_all():
        selected = [t for t in TARGETS if (selected_target == "全部" or t['name'] == selected_target)]
        total = len(selected)
        for idx, target in enumerate(selected):
            log_area.write(f"[采集] {target['name']} ...")
            try:
                record = await crawl_target(target, safe_set, concurrency=concurrency)
                if record:
                    results.append(record)
                    log_area.write(f"[采集成功] {record.get('company', '')}")
                else:
                    log_area.write(f"[采集失败] {target['name']}")
            except Exception as e:
                log_area.write(f"[采集目标异常] {target['name']} | {e}\n{traceback.format_exc()}")
            progress.progress((idx+1)/total)
    try:
        asyncio.run(run_all())
        if results:
            df = pd.DataFrame(results)
            st.success(f"采集完成，共采集到 {len(df)} 条数据！")
            st.dataframe(df)
            filename = f"采集结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            st.download_button(
                label="下载Excel结果",
                data=df.to_excel(index=False, engine='openpyxl'),
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("未采集到有效数据！")
    except Exception as e:
        st.error(f"[致命错误] 采集主流程异常: {e}\n{traceback.format_exc()}") 