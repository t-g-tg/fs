"""
ルールベースフォーム解析システム

instruction_jsonに依存しないルールベースの要素判定・入力システム
参考: ListersForm復元システムのFormAnalyzerアーキテクチャ

注意: 重い依存関係（Playwright 等）を持つモジュールは遅延インポートに変更し、
単体テスト時に不要な依存で失敗しないようにする。
"""

# 遅延インポートにより、軽量モジュールのみ即時公開（必要時に __getattr__ で解決）
__all__ = [
    'FieldPatterns',
    'ElementScorer',
    'RuleBasedAnalyzer',
    'SuccessJudge',
]

def __getattr__(name):
    if name == 'FieldPatterns':
        from .field_patterns import FieldPatterns  # type: ignore
        return FieldPatterns
    if name == 'ElementScorer':
        from .element_scorer import ElementScorer  # type: ignore
        return ElementScorer
    if name == 'RuleBasedAnalyzer':
        from .rule_based_analyzer import RuleBasedAnalyzer  # type: ignore
        return RuleBasedAnalyzer
    if name == 'SuccessJudge':
        from .success_judge import SuccessJudge  # type: ignore
        return SuccessJudge
    raise AttributeError(name)
