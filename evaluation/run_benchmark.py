#!/usr/bin/env python3
"""Unified Quantitative Benchmark Runner for Academic Paper Publication.

Runs:
1. Historical NER & Relation Extraction Benchmark (P, R, F1 across Baseline 1, 2, and Proposed Hybrid)
2. Vector RAG vs. GraphRAG vs. Docent Hybrid RAG Comparative Experiment (Fact Recall, Hallucination Rate, Token Efficiency, Latency)

Outputs:
- evaluation/results/table_ner_performance.tex
- evaluation/results/table_rag_comparison.tex
- evaluation/results/ner_evaluation_results.csv
- evaluation/results/rag_evaluation_results.csv
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def run_command(cmd: list[str], desc: str) -> bool:
    print(f"\n{'#' * 80}\n🚀 {desc}\n{'#' * 80}")
    t0 = time.time()
    res = subprocess.run(cmd)
    elapsed = time.time() - t0
    if res.returncode == 0:
        print(f"✅ {desc} 완료 (소요 시간: {elapsed:.2f}초)")
        return True
    else:
        print(f"❌ {desc} 실패 (Exit code {res.returncode})")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Run complete quantitative evaluation for journal paper.")
    parser.add_argument("--quick", action="store_true", help="Run in fast verification mode")
    parser.add_argument("--single", action="store_true", help="Run single query mode for RAG")
    parser.add_argument("--samples", type=int, default=500, help="TEI sample count for NER (default: 500)")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of RAG queries to evaluate")
    parser.add_argument("--delay", type=float, default=15.0, help="Delay in seconds between RAG API calls (default: 15.0)")
    parser.add_argument("--generate-dataset", action="store_true", help="Regenerate benchmark dataset before evaluation")
    args = parser.parse_args()

    python_bin = sys.executable

    # 0. Optional: Regenerate benchmark dataset
    if args.generate_dataset:
        gen_script = ROOT / "generate_benchmark_dataset.py"
        ok_gen = run_command([python_bin, str(gen_script), "--target-count", "60"], "0단계: 벤치마크 평가 데이터셋(60건) 자동 생성")
        if not ok_gen:
            return 1

    # 1. Run NER & Relation Extraction Benchmark
    ner_script = ROOT / "eval_ner_relation.py"
    ner_cmd = [python_bin, str(ner_script)]
    if args.quick:
        ner_cmd.append("--quick")
    else:
        ner_cmd.extend(["--samples", str(args.samples)])

    ok_ner = run_command(ner_cmd, f"1단계: 개체명 인식(NER) 및 관계 추출(RE) 정량 평가 실행 ({'30' if args.quick else args.samples}건)")
    if not ok_ner:
        return 1

    # 2. Run RAG Comparison Benchmark
    rag_script = ROOT / "eval_rag_comparison.py"
    rag_cmd = [python_bin, str(rag_script), "--delay", str(args.delay)]
    if args.single:
        rag_cmd.append("--single")
    elif args.quick:
        rag_cmd.append("--quick")
    elif args.limit > 0:
        rag_cmd.extend(["--limit", str(args.limit)])

    ok_rag = run_command(rag_cmd, "2단계: Vector RAG vs. GraphRAG vs. Docent Hybrid RAG 비교 실험 실행")
    if not ok_rag:
        return 1

    # 3. Final Summary Report
    res_dir = ROOT / "results"
    print("\n" + "=" * 80)
    print("🎉 [최종 완료] 국제 저널용 정량 평가 수치 및 LaTeX 표 생성이 완료되었습니다!")
    print("=" * 80)
    print(f"📁 생성된 결과물 목록 ({res_dir}):")
    for f in sorted(res_dir.iterdir()):
        if f.is_file():
            print(f"  - {f.name} ({f.stat().st_size} bytes)")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    sys.exit(main())
