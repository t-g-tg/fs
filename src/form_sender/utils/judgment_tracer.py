"""
フォーム送信成功判定の詳細トレーシングシステム
6段階判定プロセスの各ステップを詳細に記録し、
分析可能な形で保存する
"""

import time
import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass, asdict, field
from enum import Enum

class JudgmentStage(Enum):
    """判定ステージの定義"""
    STAGE_0_INIT = "stage0_initialization"
    STAGE_1_URL = "stage1_url_change"
    STAGE_2_MESSAGE = "stage2_success_message"
    STAGE_3_FORM = "stage3_form_disappearance"
    STAGE_4_SIBLING = "stage4_sibling_analysis"
    STAGE_5_ERROR = "stage5_error_patterns"
    STAGE_6_FAILURE = "stage6_failure_patterns"
    COMPLETE = "complete"

class JudgmentResult(Enum):
    """判定結果の定義"""
    SUCCESS = "success"
    FAILURE = "failure"
    PENDING = "pending"
    ERROR = "error"
    SKIPPED = "skipped"

@dataclass
class StageTrace:
    """各ステージのトレース情報"""
    stage: JudgmentStage
    started_at: str
    completed_at: Optional[str] = None
    duration_ms: Optional[float] = None
    result: Optional[JudgmentResult] = None
    confidence: Optional[float] = None
    details: Dict[str, Any] = field(default_factory=dict)
    patterns_matched: List[str] = field(default_factory=list)
    patterns_checked: List[str] = field(default_factory=list)
    elements_analyzed: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None
    
    def mark_completed(self, result: JudgmentResult, confidence: float = None, 
                      details: Dict[str, Any] = None, error: str = None):
        """ステージ完了をマーク"""
        self.completed_at = datetime.now(timezone(timedelta(hours=9))).isoformat()
        start_time = datetime.fromisoformat(self.started_at.replace('+09:00', ''))
        end_time = datetime.fromisoformat(self.completed_at.replace('+09:00', ''))
        self.duration_ms = (end_time - start_time).total_seconds() * 1000
        
        self.result = result
        self.confidence = confidence
        if details:
            self.details.update(details)
        self.error = error

@dataclass
class JudgmentTrace:
    """全体の判定トレース情報"""
    trace_id: str
    form_url: str
    started_at: str
    completed_at: Optional[str] = None
    total_duration_ms: Optional[float] = None
    
    # 判定結果
    final_result: Optional[JudgmentResult] = None
    final_stage: Optional[JudgmentStage] = None
    final_confidence: Optional[float] = None
    final_message: Optional[str] = None
    
    # ステージトレース
    stage_traces: List[StageTrace] = field(default_factory=list)
    
    # メタデータ
    browser_info: Dict[str, Any] = field(default_factory=dict)
    page_metadata: Dict[str, Any] = field(default_factory=dict)
    performance_metrics: Dict[str, Any] = field(default_factory=dict)
    
    def mark_completed(self, final_result: JudgmentResult, final_stage: JudgmentStage,
                      final_confidence: float = None, final_message: str = None):
        """全体完了をマーク"""
        self.completed_at = datetime.now(timezone(timedelta(hours=9))).isoformat()
        start_time = datetime.fromisoformat(self.started_at.replace('+09:00', ''))
        end_time = datetime.fromisoformat(self.completed_at.replace('+09:00', ''))
        self.total_duration_ms = (end_time - start_time).total_seconds() * 1000
        
        self.final_result = final_result
        self.final_stage = final_stage
        self.final_confidence = final_confidence
        self.final_message = final_message

class JudgmentTracer:
    """成功判定トレーシングシステム"""
    
    def __init__(self, form_url: str, enable_detailed_logging: bool = True):
        self.trace = JudgmentTrace(
            trace_id=str(uuid.uuid4()),
            form_url=form_url,
            started_at=datetime.now(timezone(timedelta(hours=9))).isoformat()
        )
        self.current_stage_trace: Optional[StageTrace] = None
        self.enable_detailed_logging = enable_detailed_logging
        
        # パフォーマンス測定用
        self._start_time = time.perf_counter()
        
    def set_browser_info(self, browser_name: str, browser_version: str, 
                        user_agent: str = None, viewport: Dict[str, int] = None):
        """ブラウザ情報を設定"""
        self.trace.browser_info = {
            "browser_name": browser_name,
            "browser_version": browser_version,
            "user_agent": user_agent,
            "viewport": viewport,
            "recorded_at": datetime.now(timezone(timedelta(hours=9))).isoformat()
        }
    
    def set_page_metadata(self, title: str, url: str, ready_state: str = None,
                         dom_elements_count: int = None):
        """ページメタデータを設定"""
        self.trace.page_metadata = {
            "title": title,
            "url": url,
            "ready_state": ready_state,
            "dom_elements_count": dom_elements_count,
            "recorded_at": datetime.now(timezone(timedelta(hours=9))).isoformat()
        }
    
    def start_stage(self, stage: JudgmentStage) -> None:
        """新しいステージの開始"""
        # 前のステージが未完了の場合は自動的に完了させる
        if self.current_stage_trace and self.current_stage_trace.completed_at is None:
            self.current_stage_trace.mark_completed(JudgmentResult.ERROR, 
                                                  error="Stage was not properly completed")
        
        # 新しいステージトレースを作成
        self.current_stage_trace = StageTrace(
            stage=stage,
            started_at=datetime.now(timezone(timedelta(hours=9))).isoformat()
        )
        self.trace.stage_traces.append(self.current_stage_trace)
    
    def add_pattern_check(self, pattern: str, matched: bool = False) -> None:
        """パターンチェック結果を記録"""
        if not self.current_stage_trace:
            return
            
        self.current_stage_trace.patterns_checked.append(pattern)
        if matched:
            self.current_stage_trace.patterns_matched.append(pattern)
    
    def add_element_analysis(self, element_type: str, element_selector: str,
                           element_text: str = None, element_attributes: Dict[str, str] = None,
                           analysis_result: str = None) -> None:
        """要素分析結果を記録"""
        if not self.current_stage_trace:
            return
            
        element_info = {
            "type": element_type,
            "selector": element_selector,
            "text": element_text[:200] if element_text else None,  # 最初の200文字のみ
            "attributes": element_attributes or {},
            "analysis_result": analysis_result,
            "analyzed_at": datetime.now(timezone(timedelta(hours=9))).isoformat()
        }
        self.current_stage_trace.elements_analyzed.append(element_info)
    
    def add_stage_detail(self, key: str, value: Any) -> None:
        """ステージの詳細情報を追加"""
        if not self.current_stage_trace:
            return
            
        self.current_stage_trace.details[key] = value
    
    def complete_stage(self, result: JudgmentResult, confidence: float = None,
                      message: str = None, additional_details: Dict[str, Any] = None) -> None:
        """現在のステージを完了"""
        if not self.current_stage_trace:
            return
            
        details = {"message": message} if message else {}
        if additional_details:
            details.update(additional_details)
            
        self.current_stage_trace.mark_completed(
            result=result,
            confidence=confidence,
            details=details
        )
    
    def complete_judgment(self, final_result: JudgmentResult, final_stage: JudgmentStage,
                         final_confidence: float = None, final_message: str = None) -> None:
        """全体の判定を完了"""
        # 現在のステージが未完了の場合は完了させる
        if self.current_stage_trace and self.current_stage_trace.completed_at is None:
            self.complete_stage(final_result, final_confidence, final_message)
        
        # パフォーマンスメトリクスを記録
        end_time = time.perf_counter()
        self.trace.performance_metrics = {
            "total_execution_time_ms": (end_time - self._start_time) * 1000,
            "stages_executed": len(self.trace.stage_traces),
            "patterns_checked_total": sum(len(st.patterns_checked) for st in self.trace.stage_traces),
            "patterns_matched_total": sum(len(st.patterns_matched) for st in self.trace.stage_traces),
            "elements_analyzed_total": sum(len(st.elements_analyzed) for st in self.trace.stage_traces)
        }
        
        # 全体を完了
        self.trace.mark_completed(final_result, final_stage, final_confidence, final_message)
    
    def get_trace_summary(self) -> Dict[str, Any]:
        """トレース概要を取得"""
        successful_stages = [st for st in self.trace.stage_traces 
                           if st.result == JudgmentResult.SUCCESS]
        failed_stages = [st for st in self.trace.stage_traces 
                        if st.result == JudgmentResult.FAILURE]
        error_stages = [st for st in self.trace.stage_traces 
                       if st.result == JudgmentResult.ERROR]
        
        return {
            "trace_id": self.trace.trace_id,
            "form_url": self.trace.form_url,
            "final_result": self.trace.final_result.value if self.trace.final_result else None,
            "final_stage": self.trace.final_stage.value if self.trace.final_stage else None,
            "final_confidence": self.trace.final_confidence,
            "total_duration_ms": self.trace.total_duration_ms,
            "stages_summary": {
                "total_stages": len(self.trace.stage_traces),
                "successful_stages": len(successful_stages),
                "failed_stages": len(failed_stages),
                "error_stages": len(error_stages)
            },
            "performance_summary": self.trace.performance_metrics,
            "completed_at": self.trace.completed_at
        }
    
    def get_detailed_report(self) -> Dict[str, Any]:
        """詳細レポートを取得"""
        return {
            "summary": self.get_trace_summary(),
            "full_trace": asdict(self.trace),
            "stage_analysis": self._analyze_stages(),
            "pattern_analysis": self._analyze_patterns(),
            "performance_analysis": self._analyze_performance()
        }
    
    def _analyze_stages(self) -> Dict[str, Any]:
        """ステージ分析"""
        stages_by_result = {}
        for stage_trace in self.trace.stage_traces:
            result = stage_trace.result.value if stage_trace.result else "unknown"
            if result not in stages_by_result:
                stages_by_result[result] = []
            stages_by_result[result].append({
                "stage": stage_trace.stage.value,
                "duration_ms": stage_trace.duration_ms,
                "confidence": stage_trace.confidence
            })
        
        return {
            "stages_by_result": stages_by_result,
            "average_stage_duration": sum(st.duration_ms or 0 for st in self.trace.stage_traces) / 
                                    len(self.trace.stage_traces) if self.trace.stage_traces else 0,
            "slowest_stage": max(self.trace.stage_traces, 
                               key=lambda st: st.duration_ms or 0).stage.value 
                               if self.trace.stage_traces else None
        }
    
    def _analyze_patterns(self) -> Dict[str, Any]:
        """パターン分析"""
        all_patterns_checked = []
        all_patterns_matched = []
        
        for stage_trace in self.trace.stage_traces:
            all_patterns_checked.extend(stage_trace.patterns_checked)
            all_patterns_matched.extend(stage_trace.patterns_matched)
        
        pattern_success_rate = {}
        for pattern in set(all_patterns_checked):
            checked_count = all_patterns_checked.count(pattern)
            matched_count = all_patterns_matched.count(pattern)
            pattern_success_rate[pattern] = {
                "checked_count": checked_count,
                "matched_count": matched_count,
                "success_rate": matched_count / checked_count if checked_count > 0 else 0
            }
        
        return {
            "total_patterns_checked": len(all_patterns_checked),
            "total_patterns_matched": len(all_patterns_matched),
            "unique_patterns_checked": len(set(all_patterns_checked)),
            "pattern_success_rates": pattern_success_rate,
            "most_successful_patterns": sorted(
                [(k, v["success_rate"]) for k, v in pattern_success_rate.items()],
                key=lambda x: x[1], reverse=True
            )[:5]
        }
    
    def _analyze_performance(self) -> Dict[str, Any]:
        """パフォーマンス分析"""
        if not self.trace.performance_metrics:
            return {}
            
        return {
            "efficiency_score": self._calculate_efficiency_score(),
            "bottlenecks": self._identify_bottlenecks(),
            "optimization_suggestions": self._generate_optimization_suggestions()
        }
    
    def _calculate_efficiency_score(self) -> float:
        """効率性スコアを計算（0-100）"""
        base_score = 100.0
        
        # 実行時間による減点
        if self.trace.total_duration_ms:
            if self.trace.total_duration_ms > 10000:  # 10秒超
                base_score -= 30
            elif self.trace.total_duration_ms > 5000:  # 5秒超
                base_score -= 15
        
        # ステージ数による減点（効率的でない場合）
        stage_count = len(self.trace.stage_traces)
        if stage_count > 4:
            base_score -= (stage_count - 4) * 5
        
        # エラーステージによる減点
        error_count = len([st for st in self.trace.stage_traces 
                          if st.result == JudgmentResult.ERROR])
        base_score -= error_count * 10
        
        return max(base_score, 0)
    
    def _identify_bottlenecks(self) -> List[Dict[str, Any]]:
        """ボトルネックを特定"""
        bottlenecks = []
        
        if not self.trace.stage_traces:
            return bottlenecks
            
        avg_duration = sum(st.duration_ms or 0 for st in self.trace.stage_traces) / len(self.trace.stage_traces)
        
        for stage_trace in self.trace.stage_traces:
            if stage_trace.duration_ms and stage_trace.duration_ms > avg_duration * 2:
                bottlenecks.append({
                    "stage": stage_trace.stage.value,
                    "duration_ms": stage_trace.duration_ms,
                    "severity": "high" if stage_trace.duration_ms > avg_duration * 3 else "medium",
                    "suggestion": f"{stage_trace.stage.value}の処理時間が平均の{stage_trace.duration_ms / avg_duration:.1f}倍です"
                })
        
        return bottlenecks
    
    def _generate_optimization_suggestions(self) -> List[str]:
        """最適化提案を生成"""
        suggestions = []
        
        # 実行時間が長い場合
        if self.trace.total_duration_ms and self.trace.total_duration_ms > 8000:
            suggestions.append("全体的な実行時間が8秒を超えています。タイムアウト値の調整を検討してください。")
        
        # エラーが多い場合
        error_count = len([st for st in self.trace.stage_traces 
                          if st.result == JudgmentResult.ERROR])
        if error_count > 1:
            suggestions.append(f"{error_count}件のステージでエラーが発生しています。例外処理の改善を検討してください。")
        
        # パターンマッチング効率
        total_checked = sum(len(st.patterns_checked) for st in self.trace.stage_traces)
        total_matched = sum(len(st.patterns_matched) for st in self.trace.stage_traces)
        
        if total_checked > 50 and total_matched / total_checked < 0.1:
            suggestions.append("パターンマッチング成功率が低いです。パターンの見直しを検討してください。")
        
        return suggestions
    
    def export_trace_json(self) -> str:
        """トレース情報をJSON形式でエクスポート"""
        # Enumを文字列に変換
        def enum_to_str(obj):
            if isinstance(obj, Enum):
                return obj.value
            raise TypeError(f"Object {obj} is not JSON serializable")
        
        return json.dumps(asdict(self.trace), indent=2, ensure_ascii=False, 
                         default=enum_to_str)