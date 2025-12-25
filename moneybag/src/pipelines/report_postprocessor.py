import os
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[3]

class ReportPostProcessor:
    """
    생성된 마크다운 리포트를 읽고, 전략 다양성 페널티를 적용하고,
    콘텐츠를 동적으로 재작성하는 후처리 클래스.
    """
    def __init__(self):
        """초기화"""
        self.out_dir = BASE_DIR / "moneybag" / "data" / "out"

    def _parse_top_strategy_from_md(self, file_path):
        """마크다운 파일에서 1위 전략명을 추출합니다."""
        try:
            content = Path(file_path).read_text(encoding='utf-8')
            match = re.search(r'\|\s*1\s*\|\s*([^|]+?)\s*\|', content)
            if match:
                return match.group(1).strip()
        except Exception as e:
            print(f"⚠️ [Parser] '{file_path.name}' 파일 파싱 실패: {e}")
        return None

    def _get_strategy_history(self, days=2):
        """최근 리포트에서 1위 전략 이력을 가져옵니다."""
        history = []
        try:
            files = sorted(self.out_dir.glob("SecretNote_*.md"), key=os.path.getmtime, reverse=True)
            history_files = files[1:days+1] # 오늘 생성된 파일 제외
            for f in history_files:
                top_strategy = self._parse_top_strategy_from_md(f)
                if top_strategy:
                    history.append(top_strategy)
            print(f"📈 [History] 최근 상위 전략: {history}")
        except Exception as e:
            print(f"⚠️ [History] 과거 전략 이력 로딩 실패: {e}")
        return history

    def _apply_diversity_penalty(self, current_candidates, history_logs):
        """최근 노출 이력을 기반으로 전략 점수에 페널티를 부여합니다."""
        PENALTY_WEIGHTS = {"yesterday": 0.8, "day_before": 0.9}
        final_scores = current_candidates.copy()
        for strategy_name in final_scores.keys():
            if len(history_logs) > 0 and strategy_name == history_logs[0]:
                final_scores[strategy_name] *= PENALTY_WEIGHTS["yesterday"]
            if len(history_logs) > 1 and strategy_name == history_logs[1]:
                final_scores[strategy_name] *= PENALTY_WEIGHTS["day_before"]
        return dict(sorted(final_scores.items(), key=lambda x: x[1], reverse=True))

    def _generate_new_conclusion(self, top_3_strategies):
        """상위 3개 전략 비교 결론을 생성합니다."""
        if not top_3_strategies: return ""
        conclusion_parts = ["\n\n## 💡 최종 결론 (The Verdict)\n"]
        top_1 = top_3_strategies[0]
        conclusion_parts.append(f"**금일 시장 분석 결과, '{top_1['name']}' 전략이 가장 높은 점수를 획득했습니다.** {top_1['description']}\n")
        if len(top_3_strategies) > 1:
            conclusion_parts.append("\n### 🎯 차선책 분석\n")
            for i, strategy in enumerate(top_3_strategies[1:], start=2):
                conclusion_parts.append(f"**{i}순위 대안: '{strategy['name']}' ({strategy['type']})**")
                conclusion_parts.append(f"- **주요 특징:** {strategy['description']}")
                if "Trend" in strategy['type']:
                    conclusion_parts.append("- **고려사항:** 추세가 명확할 때 높은 신뢰도를 보이지만, 횡보장에서는 잦은 손실이 발생할 수 있습니다.\n")
                elif "Mean Reversion" in strategy['type']:
                     conclusion_parts.append("- **고려사항:** 변동성이 큰 박스권 장세에 유리하나, 강한 추세가 시작될 경우 추세에 역행하는 위험이 있습니다.\n")
                else:
                    conclusion_parts.append("- **고려사항:** 특정 조건에서 유효한 전략으로, 시장 상황 변화에 대한 지속적인 모니터링이 필요합니다.\n")
        conclusion_parts.append("\n> **투자 조언:** 1위 전략을 중심으로 대응하되, 시장 상황이 변할 경우 차선책으로 제시된 전략들의 시나리오를 염두에 두는 유연한 접근이 필요합니다.")
        return "\n".join(conclusion_parts)

    def run(self, md_path: Path):
        """주요 실행 함수: 마크다운 파일을 읽고, 페널티 적용 후 다시 씁니다."""
        history_logs = self._get_strategy_history(days=2)
        if not md_path or not md_path.exists():
            print("❌ [PostProcessor] 처리할 마크다운 파일이 없습니다.")
            return

        try:
            content = md_path.read_text(encoding='utf-8')
            table_regex = re.compile(r"(\s*\|\s*순위\s*\|.*?\|[\s\r\n]+.*?\|[\s\r\n]+(?:\|\s*\d+\s*\|.*?\|[\s\r\n]*)+)")
            table_match = table_regex.search(content)
            if not table_match: return

            original_table_str = table_match.group(1)
            table_rows = [row for row in original_table_str.strip().split('\n') if row.strip()]
            header, separator, strategy_rows = table_rows[0], table_rows[1], table_rows[2:]

            current_candidates, parsed_strategies = {}, []
            for row in strategy_rows:
                parts = [p.strip() for p in row.split('|') if p.strip()]
                if len(parts) < 4: continue
                name, score_str = parts[1], parts[3]
                try:
                    current_candidates[name] = float(score_str)
                    parsed_strategies.append({'name': name, 'type': parts[2], 'score': float(score_str), 'description': parts[4] if len(parts) > 4 else ""})
                except (ValueError, IndexError): continue

            penalized_scores = self._apply_diversity_penalty(current_candidates, history_logs)

            new_table_rows, top_3_strategies_after_penalty = [header, separator], []
            for i, (name, score) in enumerate(penalized_scores.items()):
                info = next((s for s in parsed_strategies if s['name'] == name), None)
                if info:
                    new_table_rows.append(f"| {i+1} | {name} | {info['type']} | {int(round(score))} | {info['description']} |")
                    if i < 3: top_3_strategies_after_penalty.append(info)
            
            new_content = content.replace(original_table_str.strip(), "\n".join(new_table_rows))
            
            new_conclusion_str = self._generate_new_conclusion(top_3_strategies_after_penalty)
            if new_conclusion_str:
                conclusion_regex = re.compile(r"(##\s*(?:💡\s*)?(?:최종 결론|The Verdict).*?)(?=##|$)", re.DOTALL)
                new_content = conclusion_regex.sub(new_conclusion_str, new_content) if conclusion_regex.search(new_content) else new_content + new_conclusion_str
                print("🔄 [Rewrite] '최종 결론' 섹션을 동적으로 업데이트했습니다.")

            md_path.write_text(new_content, encoding='utf-8')
            print("✅ [PostProcessor] 전략 페널티 적용 및 마크다운 파일 업데이트 완료.")
        except Exception as e:
            print(f"❌ [PostProcessor] 페널티 적용 중 오류 발생: {e}")