import logging
from collections.abc import Mapping
from typing import Any

from flask import current_app
from flask_login import login_user

from core.app.entities.app_invoke_entities import InvokeFrom
from core.rag.extractor.extractor_base import BaseExtractor
from core.rag.models.document import Document
from extensions.ext_database import db
from models.account import Account
from models.model import App, Tenant

logger = logging.getLogger(__name__)

class AppExtractor(BaseExtractor):
    def __init__(self, app_id: str, user_id: str, tenant_id: str, inputs: Mapping[str, Any]):
        self._app_id = app_id
        self._user_id = user_id
        self._tenant_id = tenant_id
        self._inputs = inputs

    def extract(self) -> list[Document]:
        from services.app_generate_service import AppGenerateService

        if not all([self._app_id, self._user_id, self._tenant_id]):
            raise ValueError("Workflow extraction requires app_id, user_id, and tenant_id.")

        user = db.session.query(Account).filter(Account.id == self._user_id).first()
        if not user:
            raise ValueError(f"User with ID {self._user_id} not found.")

        tenant = db.session.query(Tenant).filter(Tenant.id == self._tenant_id).first()
        if not tenant:
            raise ValueError(f"Tenant with ID {self._tenant_id} not found.")

        user.current_tenant = tenant

        app_model = db.session.query(App).filter(
            App.id == self._app_id,
            App.tenant_id == self._tenant_id,
        ).first()

        if not app_model:
            raise ValueError(f"Workflow {self._app_id} not found or is not a workflow app.")

        args: Mapping[str, Any] = {
            "inputs": self._inputs,
        }

        with current_app.test_request_context():
            login_user(user)

            result_dict = AppGenerateService.generate(
                app_model=app_model,
                user=user,
                args=args,
                invoke_from=InvokeFrom.WEB_APP,
                streaming=False,
            )
        data_payload = result_dict.get('data', {})  # type: ignore
        outputs = data_payload.get('outputs', {})

        # 规定 app 的输出必须包含 'text' 字段
        extracted_text = outputs.get('text', '')

        if not extracted_text:
            logging.warning("Workflow %s succeeded but produced no 'text' output.", self._app_id)
            return []

        return [Document(page_content=str(extracted_text))]
