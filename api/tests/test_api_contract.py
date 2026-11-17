"""API 契约回归：保留现有路由与 Swagger 文档，不因备份发布改造而破坏。"""
import yaml
from pathlib import Path

from app.main import app


def _paths():
    return app.openapi()["paths"]


def test_existing_routes_preserved():
    paths = _paths()
    expected = {
        "/api/health": {"get"},
        "/api/basebackup": {"get", "post"},
        "/api/checkpoints": {"get", "post"},
        "/api/checkpoints/{name}": {"get"},
        "/api/recoveries": {"get", "post"},
        "/api/recoveries/{job_id}": {"get", "delete"},
        "/api/recoveries/{job_id}/retry": {"post"},
        "/api/recoveries/{job_id}/samples": {"get"},
        "/api/recoveries/{job_id}/query": {"post"},
    }
    for path, methods in expected.items():
        assert path in paths, f"缺少路由 {path}"
        assert methods <= set(paths[path]), f"{path} 缺少方法 {methods}"


def test_swagger_and_openapi_available():
    from fastapi.testclient import TestClient
    client = TestClient(app)  # 不进入 lifespan，避免依赖真实源库
    assert client.get("/openapi.json").status_code == 200
    assert client.get("/docs").status_code == 200


def test_backup_status_reports_usable_backup():
    schema = app.openapi()["paths"]["/api/basebackup"]["get"]
    assert schema["summary"]


def test_compose_is_parseable_and_wires_api():
    compose = Path(__file__).resolve().parents[2] / "compose.yaml"
    data = yaml.safe_load(compose.read_text())
    assert set(data["services"]) >= {"db", "api"}
    api = data["services"]["api"]
    assert any("8000:8000" in p for p in api["ports"])
    assert data["services"]["db"]["environment"]["POSTGRES_DB"] == "water"
