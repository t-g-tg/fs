import logging
from typing import Dict, List, Any
from playwright.async_api import Page, Locator

from .form_structure_analyzer import FormElement

logger = logging.getLogger(__name__)

class ElementClassifier:
    """フォーム要素の収集と分類を担当するクラス"""

    def __init__(self, page: Page, settings: Dict[str, Any]):
        self.page = page
        self.settings = settings
        self.special_elements: Dict[str, List] = {'checkboxes': [], 'radios': [], 'selects': [], 'textareas': [], 'buttons': []}

    async def classify_structured_elements(self, structured_elements: List[FormElement]) -> Dict[str, List[Locator]]:
        classified = {
            'text_inputs': [], 'email_inputs': [], 'tel_inputs': [], 'number_inputs': [],
            'url_inputs': [], 'textareas': [], 'selects': [], 'checkboxes': [],
            'radios': [], 'other_inputs': []
        }
        for el in structured_elements:
            if el.tag_name == 'textarea':
                classified['textareas'].append(el.locator)
            elif el.tag_name == 'select':
                classified['selects'].append(el.locator)
            elif el.tag_name == 'input':
                if el.element_type in ['', 'text']: classified['text_inputs'].append(el.locator)
                elif el.element_type in ['email', 'mail']: classified['email_inputs'].append(el.locator)
                elif el.element_type == 'tel': classified['tel_inputs'].append(el.locator)
                elif el.element_type == 'number': classified['number_inputs'].append(el.locator)
                elif el.element_type == 'url': classified['url_inputs'].append(el.locator)
                elif el.element_type == 'checkbox': classified['checkboxes'].append(el.locator)
                elif el.element_type == 'radio': classified['radios'].append(el.locator)
                else: classified['other_inputs'].append(el.locator)
        
        self.special_elements.update({k: classified[k] for k in self.special_elements if k in classified})
        return classified

    def get_classification_summary(self, classified: Dict[str, List]) -> str:
        return ", ".join([f"{k}:{len(v)}" for k, v in classified.items() if v])
