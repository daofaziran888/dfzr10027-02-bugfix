"""Pytest 配置与共享 fixture。"""
from __future__ import annotations

import pytest

from app.store import reset_store


@pytest.fixture(autouse=True)
def fresh_store():
    """每个测试用例前重置全局 store，保证隔离。"""
    reset_store()
    yield
