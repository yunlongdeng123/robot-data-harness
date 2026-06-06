"""dataset_card.md 渲染（蒸馏数据集卡片）。"""

from __future__ import annotations

from robot_dh.distill.report import DistillReport


def render_dataset_card(report: DistillReport) -> str:
    """根据 DistillReport 渲染中文 dataset card（markdown）。"""
    lines = [
        f"# 蒸馏数据集卡片：{report.distill_id}",
        "",
        "## 概览",
        "",
        f"- 蒸馏格式（distill_format）：`{report.distill_format}`",
        f"- teacher 模型：`{report.teacher_model_id or '未知'}`",
        f"- 源推理任务：`{report.source_inference_job_id or '未知'}`",
        f"- 数据集：`{report.dataset_id or '未知'}` / 版本 `{report.version or '未知'}`",
        f"- 输出根：`{report.output_uri}`",
        "",
        "## 切分统计",
        "",
        "| split | 样本数 | 文件 |",
        "| --- | --- | --- |",
        f"| train | {report.num_train} | `{report.train_uri}` |",
        f"| val | {report.num_val} | `{report.val_uri}` |",
        f"| test | {report.num_test} | `{report.test_uri}` |",
        f"| 合计 | {report.num_total} | — |",
        "",
        f"- 跳过（缺字段 / 非 OK）：{report.num_skipped}",
        f"- 切分比例：{report.split_ratio}",
        f"- 状态：{report.status}",
        "",
        "## 说明",
        "",
        "本数据集由 teacher 模型推理结果（pseudo labels）蒸馏而来，仅用于下游 student 训练 / 评测；",
        "标签质量受 teacher 能力限制，使用前请结合 `confidence` 与人工抽检。",
    ]
    return "\n".join(lines) + "\n"
