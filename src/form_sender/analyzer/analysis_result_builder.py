import logging
from typing import Dict, Any, List

from .field_patterns import FieldPatterns
from .element_scorer import ElementScorer

logger = logging.getLogger(__name__)

class AnalysisResultBuilder:
    """解析結果の整形を担当するクラス"""

    def __init__(self, field_patterns: FieldPatterns, element_scorer: ElementScorer, settings: Dict[str, Any]):
        self.field_patterns = field_patterns
        self.element_scorer = element_scorer
        self.settings = settings

    def create_analysis_summary(self, field_mapping: Dict[str, Any], 
                                auto_handled: Dict[str, Any], 
                                special_elements: Dict[str, List[Any]],
                                form_type: str | None = None) -> Dict[str, Any]:
        total_patterns = len(self.field_patterns.get_patterns())
        mapped_count = len(field_mapping)
        auto_handled_count = len(auto_handled)
        
        important_fields = self.settings.get('essential_fields', []) + ['会社名', '姓', '名']
        mapped_important = sum(1 for field in important_fields if field in field_mapping)
        
        # 認証フォームは『営業フォームではない』ため、成功/重要項目の指標を N/A 扱いにする
        is_auth = (form_type or '').lower() == 'auth_form'
        summary = {
            'total_field_patterns': total_patterns,
            'mapped_fields': mapped_count,
            'auto_handled_fields': auto_handled_count,
            'mapping_coverage': f"{mapped_count}/{total_patterns} ({mapped_count/total_patterns*100:.1f}%)" if total_patterns > 0 else "N/A",
            'important_fields_mapped': "N/A" if is_auth else f"{mapped_important}/{len(important_fields)}",
            'special_elements_count': {k: len(v) for k, v in special_elements.items()},
            'analysis_success': True if is_auth else (mapped_count > 0 or auto_handled_count > 0),
            'form_type': form_type or ''
        }

        return summary

    def create_debug_info(self, unmapped_elements: List[Any]) -> Dict[str, Any]:
        return {
            'settings': self.settings,
            'unmapped_elements_count': len(unmapped_elements),
            'field_patterns_loaded': len(self.field_patterns.get_patterns()),
            'scorer_weights': self.element_scorer.SCORE_WEIGHTS
        }
