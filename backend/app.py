import datetime as dt
import json
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import jwt
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from neo4j import GraphDatabase
from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
    select,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker
from werkzeug.security import check_password_hash, generate_password_hash


load_dotenv()


def _env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name)
    if v is None or v == "":
        if default is None:
            raise RuntimeError(f"Missing environment variable: {name}")
        return default
    return v


@dataclass(frozen=True)
class Settings:
    secret_key: str
    jwt_secret: str
    database_url: str
    neo4j_uri: Optional[str]
    neo4j_user: Optional[str]
    neo4j_password: Optional[str]
    upload_dir: Path
    cors_origins: List[str]

    @staticmethod
    def from_env() -> "Settings":
        cors_origins = [s.strip() for s in os.getenv("CORS_ORIGINS", "http://localhost:5173").split(",") if s.strip()]
        upload_dir = Path(os.getenv("UPLOAD_DIR", str(Path(__file__).parent / "storage"))).resolve()
        upload_dir.mkdir(parents=True, exist_ok=True)

        neo4j_uri = os.getenv("NEO4J_URI")
        neo4j_user = os.getenv("NEO4J_USER")
        neo4j_password = os.getenv("NEO4J_PASSWORD")
        if not (neo4j_uri and neo4j_user and neo4j_password):
            neo4j_uri = None
            neo4j_user = None
            neo4j_password = None

        return Settings(
            secret_key=os.getenv("FLASK_SECRET_KEY", "dev-secret"),
            jwt_secret=os.getenv("JWT_SECRET", "dev-jwt-secret"),
            database_url=os.getenv("DATABASE_URL", "sqlite:///./dev.db"),
            neo4j_uri=neo4j_uri,
            neo4j_user=neo4j_user,
            neo4j_password=neo4j_password,
            upload_dir=upload_dir,
            cors_origins=cors_origins,
        )


naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=naming_convention)


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(256), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())

    roles: Mapped[List["Role"]] = relationship("Role", secondary="user_roles", back_populates="users")
    behaviors: Mapped[List["UserBehavior"]] = relationship("UserBehavior", back_populates="user")


class Role(Base):
    __tablename__ = "roles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(32), unique=True, nullable=False)

    users: Mapped[List[User]] = relationship("User", secondary="user_roles", back_populates="roles")
    permissions: Mapped[List["Permission"]] = relationship(
        "Permission", secondary="role_permissions", back_populates="roles"
    )


class Permission(Base):
    __tablename__ = "permissions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)

    roles: Mapped[List[Role]] = relationship("Role", secondary="role_permissions", back_populates="permissions")


class UserRole(Base):
    __tablename__ = "user_roles"
    __table_args__ = (UniqueConstraint("user_id", "role_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    role_id: Mapped[int] = mapped_column(ForeignKey("roles.id"), nullable=False)


class RolePermission(Base):
    __tablename__ = "role_permissions"
    __table_args__ = (UniqueConstraint("role_id", "permission_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    role_id: Mapped[int] = mapped_column(ForeignKey("roles.id"), nullable=False)
    permission_id: Mapped[int] = mapped_column(ForeignKey("permissions.id"), nullable=False)


class Resource(Base):
    __tablename__ = "resources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    file_name: Mapped[str] = mapped_column(String(260), nullable=False)
    file_path: Mapped[str] = mapped_column(String(600), nullable=False)
    file_type: Mapped[str] = mapped_column(String(16), nullable=False)
    course: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    knowledge_point: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    created_by: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    status: Mapped[str] = mapped_column(
        Enum("pending", "approved", "rejected", name="resource_status"),
        nullable=False,
        default="pending",
    )
    audit_comment: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    tags: Mapped[List["ResourceTag"]] = relationship("ResourceTag", back_populates="resource", cascade="all, delete-orphan")


class ResourceTag(Base):
    __tablename__ = "resource_tags"
    __table_args__ = (UniqueConstraint("resource_id", "tag"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    resource_id: Mapped[int] = mapped_column(ForeignKey("resources.id"), nullable=False)
    tag: Mapped[str] = mapped_column(String(64), nullable=False)

    resource: Mapped[Resource] = relationship("Resource", back_populates="tags")


class UserBehavior(Base):
    __tablename__ = "user_behaviors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    resource_id: Mapped[int] = mapped_column(ForeignKey("resources.id"), nullable=False)
    action: Mapped[str] = mapped_column(Enum("view", "favorite", "unfavorite", name="behavior_action"), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())

    user: Mapped[User] = relationship("User", back_populates="behaviors")


class SystemLog(Base):
    __tablename__ = "system_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    method: Mapped[str] = mapped_column(String(12), nullable=False)
    path: Mapped[str] = mapped_column(String(300), nullable=False)
    status_code: Mapped[int] = mapped_column(Integer, nullable=False)
    meta: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())


def _create_app() -> Flask:
    settings = Settings.from_env()

    app = Flask(__name__)
    app.config["SECRET_KEY"] = settings.secret_key
    CORS(app, resources={r"/api/*": {"origins": settings.cors_origins}}, supports_credentials=True)

    engine = create_engine(settings.database_url, future=True)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, class_=Session)

    neo4j_driver = None
    if settings.neo4j_uri:
        neo4j_driver = GraphDatabase.driver(
            settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password)
        )

    def init_db() -> None:
        Base.metadata.create_all(engine)
        with SessionLocal() as db:
            _seed_rbac(db)

    def make_token(user: User) -> str:
        now = dt.datetime.utcnow()
        payload = {
            "sub": str(user.id),
            "username": user.username,
            "roles": [r.name for r in user.roles],
            "iat": int(now.timestamp()),
            "exp": int((now + dt.timedelta(hours=24)).timestamp()),
        }
        return jwt.encode(payload, settings.jwt_secret, algorithm="HS256")

    def decode_token(token: str) -> Dict[str, Any]:
        return jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])

    def get_current_user(db: Session) -> Optional[User]:
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return None
        token = auth[len("Bearer ") :].strip()
        try:
            payload = decode_token(token)
        except Exception:
            return None
        user_id = int(payload.get("sub"))
        return db.get(User, user_id)

    def require_auth(db: Session) -> User:
        user = get_current_user(db)
        if not user:
            raise ApiError("UNAUTHORIZED", "Missing or invalid token", 401)
        if not user.is_active:
            raise ApiError("FORBIDDEN", "User disabled", 403)
        return user

    def require_roles(user: User, allowed: Iterable[str]) -> None:
        user_roles = {r.name for r in user.roles}
        if not user_roles.intersection(set(allowed)):
            raise ApiError("FORBIDDEN", "Insufficient role", 403)

    @app.after_request
    def _log_request(resp):
        if request.path.startswith("/api/") and request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            try:
                with SessionLocal() as db:
                    user = get_current_user(db)
                    meta: Dict[str, Any] = {"remote_addr": request.remote_addr}
                    db.add(
                        SystemLog(
                            user_id=user.id if user else None,
                            method=request.method,
                            path=request.path,
                            status_code=resp.status_code,
                            meta=meta,
                        )
                    )
                    db.commit()
            except Exception:
                pass
        return resp

    @app.errorhandler(ApiError)
    def _handle_api_error(err: "ApiError"):
        return jsonify({"error": {"code": err.code, "message": err.message}}), err.http_status

    @app.get("/api/health")
    def health():
        return jsonify({"ok": True})

    @app.post("/api/auth/register")
    def register():
        data = _json()
        username = str(data.get("username", "")).strip()
        password = str(data.get("password", "")).strip()
        role = str(data.get("role", "student")).strip() or "student"
        if not username or not password:
            raise ApiError("BAD_REQUEST", "username/password required", 400)

        with SessionLocal() as db:
            new_user = User(username=username, password_hash=generate_password_hash(password))
            db.add(new_user)
            try:
                db.flush()
            except IntegrityError:
                db.rollback()
                raise ApiError("CONFLICT", "username already exists", 409)

            r = db.execute(select(Role).where(Role.name == role)).scalar_one_or_none()
            if r is None:
                r = db.execute(select(Role).where(Role.name == "student")).scalar_one()
            new_user.roles.append(r)
            db.commit()
            db.refresh(new_user)

            return jsonify({"token": make_token(new_user), "user": _user_dto(new_user)})

    @app.post("/api/auth/login")
    def login():
        data = _json()
        username = str(data.get("username", "")).strip()
        password = str(data.get("password", "")).strip()
        if not username or not password:
            raise ApiError("BAD_REQUEST", "username/password required", 400)

        with SessionLocal() as db:
            user = db.execute(select(User).where(User.username == username)).scalar_one_or_none()
            if not user or not check_password_hash(user.password_hash, password):
                raise ApiError("UNAUTHORIZED", "Invalid credentials", 401)
            if not user.is_active:
                raise ApiError("FORBIDDEN", "User disabled", 403)
            return jsonify({"token": make_token(user), "user": _user_dto(user)})

    @app.get("/api/me")
    def me():
        with SessionLocal() as db:
            user = require_auth(db)
            return jsonify({"user": _user_dto(user)})

    @app.post("/api/resources/upload")
    def upload_resource():
        with SessionLocal() as db:
            user = require_auth(db)

            if "file" not in request.files:
                raise ApiError("BAD_REQUEST", "file is required", 400)
            f = request.files["file"]
            title = (request.form.get("title") or f.filename or "").strip()
            description = (request.form.get("description") or "").strip() or None
            course = (request.form.get("course") or "").strip() or None
            knowledge_point = (request.form.get("knowledge_point") or "").strip() or None
            tags_raw = (request.form.get("tags") or "").strip()

            if not title:
                raise ApiError("BAD_REQUEST", "title is required", 400)

            original_name = (f.filename or "upload").strip()
            suffix = Path(original_name).suffix.lower()
            if suffix not in {".pdf", ".doc", ".docx"}:
                raise ApiError("BAD_REQUEST", "Only PDF/Word supported", 400)

            file_id = str(uuid.uuid4())
            safe_name = f"{file_id}{suffix}"
            dest_path = settings.upload_dir / safe_name
            f.save(dest_path)

            res = Resource(
                title=title,
                description=description,
                file_name=original_name,
                file_path=str(dest_path),
                file_type=suffix.lstrip("."),
                course=course,
                knowledge_point=knowledge_point,
                created_by=user.id,
                status="pending",
            )
            db.add(res)
            db.flush()

            tags = _split_tags(tags_raw)
            for t in tags:
                res.tags.append(ResourceTag(tag=t))
            db.commit()
            db.refresh(res)
            return jsonify({"resource": _resource_dto(res)})

    @app.get("/api/resources")
    def list_resources():
        status = (request.args.get("status") or "").strip()
        keyword = (request.args.get("keyword") or "").strip()
        tag = (request.args.get("tag") or "").strip()

        with SessionLocal() as db:
            q = select(Resource).order_by(Resource.created_at.desc())
            if status in {"pending", "approved", "rejected"}:
                q = q.where(Resource.status == status)
            if keyword:
                like = f"%{keyword}%"
                q = q.where((Resource.title.like(like)) | (Resource.description.like(like)))
            resources = db.execute(q).scalars().all()
            if tag:
                resources = [r for r in resources if tag in {t.tag for t in r.tags}]
            return jsonify({"items": [_resource_dto(r) for r in resources]})

    @app.get("/api/resources/<int:resource_id>/download")
    def download_resource(resource_id: int):
        with SessionLocal() as db:
            user = require_auth(db)
            res = db.get(Resource, resource_id)
            if not res:
                raise ApiError("NOT_FOUND", "resource not found", 404)
            if res.status != "approved" and "admin" not in {r.name for r in user.roles}:
                raise ApiError("FORBIDDEN", "resource not approved", 403)
            if res.status == "approved":
                db.add(UserBehavior(user_id=user.id, resource_id=res.id, action="view"))
                db.commit()

            p = Path(res.file_path)
            return send_from_directory(p.parent, p.name, as_attachment=True, download_name=res.file_name)

    @app.patch("/api/resources/<int:resource_id>/audit")
    def audit_resource(resource_id: int):
        data = _json()
        status = str(data.get("status", "")).strip()
        comment = (data.get("comment") or "").strip() or None
        if status not in {"approved", "rejected"}:
            raise ApiError("BAD_REQUEST", "status must be approved/rejected", 400)

        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"admin"})

            res = db.get(Resource, resource_id)
            if not res:
                raise ApiError("NOT_FOUND", "resource not found", 404)
            res.status = status
            res.audit_comment = comment
            db.commit()
            db.refresh(res)
            return jsonify({"resource": _resource_dto(res)})

    @app.post("/api/resources/<int:resource_id>/tags")
    def set_resource_tags(resource_id: int):
        data = _json()
        tags = data.get("tags")
        if not isinstance(tags, list) or not all(isinstance(t, str) for t in tags):
            raise ApiError("BAD_REQUEST", "tags must be string[]", 400)
        normalized = _split_tags(",".join(tags))

        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"admin", "teacher"})

            res = db.get(Resource, resource_id)
            if not res:
                raise ApiError("NOT_FOUND", "resource not found", 404)
            res.tags.clear()
            for t in normalized:
                res.tags.append(ResourceTag(tag=t))
            db.commit()
            db.refresh(res)
            return jsonify({"resource": _resource_dto(res)})

    @app.post("/api/resources/<int:resource_id>/favorite")
    def favorite(resource_id: int):
        data = _json()
        action = str(data.get("action", "favorite")).strip()
        if action not in {"favorite", "unfavorite"}:
            raise ApiError("BAD_REQUEST", "action must be favorite/unfavorite", 400)

        with SessionLocal() as db:
            user = require_auth(db)
            res = db.get(Resource, resource_id)
            if not res:
                raise ApiError("NOT_FOUND", "resource not found", 404)
            db.add(UserBehavior(user_id=user.id, resource_id=resource_id, action=action))
            db.commit()
            return jsonify({"ok": True})

    @app.get("/api/search")
    def semantic_search():
        keyword = (request.args.get("keyword") or "").strip()
        knowledge = (request.args.get("knowledge") or "").strip()

        with SessionLocal() as db:
            q = select(Resource).where(Resource.status == "approved").order_by(Resource.created_at.desc())
            if keyword:
                like = f"%{keyword}%"
                q = q.where((Resource.title.like(like)) | (Resource.description.like(like)))
            items = db.execute(q).scalars().all()

            related_resource_ids: Optional[set[int]] = None
            if knowledge and neo4j_driver:
                related_resource_ids = set(_neo4j_search_resources(neo4j_driver, knowledge))

            if related_resource_ids is not None:
                items = [r for r in items if r.id in related_resource_ids]

            return jsonify({"items": [_resource_dto(r) for r in items]})

    @app.get("/api/recommendations")
    def recommendations():
        with SessionLocal() as db:
            user = require_auth(db)
            items = _recommend(db, user_id=user.id, neo4j_driver=neo4j_driver)
            return jsonify({"items": items})

    @app.get("/api/graph/overview")
    def graph_overview():
        course = (request.args.get("course") or "").strip() or None
        if neo4j_driver:
            return jsonify(_neo4j_overview(neo4j_driver, course=course))

        with SessionLocal() as db:
            q = select(Resource).where(Resource.status == "approved").order_by(Resource.created_at.desc())
            if course:
                q = q.where(Resource.course == course)
            resources = db.execute(q).scalars().all()

            nodes: Dict[str, Dict[str, Any]] = {}
            links: List[Dict[str, Any]] = []

            def upsert_node(node_id: str, label: str, ntype: str) -> None:
                nodes.setdefault(node_id, {"id": node_id, "label": label, "type": ntype})

            for r in resources:
                if r.course:
                    upsert_node(f"course:{r.course}", r.course, "course")
                if r.knowledge_point:
                    upsert_node(f"kp:{r.knowledge_point}", r.knowledge_point, "knowledge_point")
                upsert_node(f"res:{r.id}", r.title, "resource")

                if r.course:
                    links.append({"source": f"course:{r.course}", "target": f"res:{r.id}", "type": "has"})
                if r.knowledge_point:
                    links.append({"source": f"kp:{r.knowledge_point}", "target": f"res:{r.id}", "type": "related"})

            return jsonify({"nodes": list(nodes.values()), "links": links, "source": "mysql"})

    @app.post("/api/graph/import")
    def graph_import():
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"admin"})

        if not neo4j_driver:
            raise ApiError("SERVICE_UNAVAILABLE", "Neo4j not configured", 503)
        payload = _json()
        stats = _neo4j_import(neo4j_driver, payload)
        return jsonify({"ok": True, "stats": stats})

    @app.get("/api/admin/logs")
    def get_logs():
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"admin"})
            logs = db.execute(select(SystemLog).order_by(SystemLog.created_at.desc()).limit(200)).scalars().all()
            items = [
                {
                    "id": l.id,
                    "user_id": l.user_id,
                    "method": l.method,
                    "path": l.path,
                    "status_code": l.status_code,
                    "meta": l.meta,
                    "created_at": l.created_at.isoformat(),
                }
                for l in logs
            ]
            return jsonify({"items": items})

    @app.post("/api/admin/backup")
    def backup():
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"admin"})
        return jsonify({"ok": True, "message": "backup endpoint stub"})

    app.init_db = init_db  # type: ignore[attr-defined]
    app.settings = settings  # type: ignore[attr-defined]
    return app


class ApiError(Exception):
    def __init__(self, code: str, message: str, http_status: int):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status


def _json() -> Dict[str, Any]:
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        raise ApiError("BAD_REQUEST", "JSON body required", 400)
    return data


def _seed_rbac(db: Session) -> None:
    existing_roles = {r.name for r in db.execute(select(Role)).scalars().all()}
    for role_name in ["student", "teacher", "admin"]:
        if role_name not in existing_roles:
            db.add(Role(name=role_name))
    db.commit()


def _split_tags(raw: str) -> List[str]:
    if not raw:
        return []
    parts = re.split(r"[,，\s]+", raw)
    out: List[str] = []
    seen = set()
    for p in parts:
        t = p.strip()
        if not t:
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(t[:64])
    return out[:20]


def _user_dto(u: User) -> Dict[str, Any]:
    return {"id": u.id, "username": u.username, "roles": [r.name for r in u.roles]}


def _resource_dto(r: Resource) -> Dict[str, Any]:
    return {
        "id": r.id,
        "title": r.title,
        "description": r.description,
        "file_name": r.file_name,
        "file_type": r.file_type,
        "course": r.course,
        "knowledge_point": r.knowledge_point,
        "status": r.status,
        "audit_comment": r.audit_comment,
        "tags": [t.tag for t in r.tags],
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "updated_at": r.updated_at.isoformat() if r.updated_at else None,
    }


def _neo4j_overview(driver, course: Optional[str]) -> Dict[str, Any]:
    query = """
    MATCH (c:Course)-[:HAS_KP]->(k:KnowledgePoint)
    OPTIONAL MATCH (k)-[:RELATED_RESOURCE]->(r:Resource)
    WITH c,k,r
    WHERE $course IS NULL OR c.name = $course
    RETURN c.name as course, k.name as kp, collect(distinct r.id) as resource_ids
    """
    nodes: Dict[str, Dict[str, Any]] = {}
    links: List[Dict[str, Any]] = []

    def upsert_node(node_id: str, label: str, ntype: str) -> None:
        nodes.setdefault(node_id, {"id": node_id, "label": label, "type": ntype})

    with driver.session() as session:
        records = session.run(query, {"course": course}).data()
    for rec in records:
        c = rec.get("course")
        k = rec.get("kp")
        if not c or not k:
            continue
        upsert_node(f"course:{c}", c, "course")
        upsert_node(f"kp:{k}", k, "knowledge_point")
        links.append({"source": f"course:{c}", "target": f"kp:{k}", "type": "has_kp"})
        for rid in rec.get("resource_ids") or []:
            upsert_node(f"res:{rid}", str(rid), "resource")
            links.append({"source": f"kp:{k}", "target": f"res:{rid}", "type": "related_resource"})

    return {"nodes": list(nodes.values()), "links": links, "source": "neo4j"}


def _neo4j_import(driver, payload: Dict[str, Any]) -> Dict[str, Any]:
    courses = payload.get("courses") or []
    knowledge_points = payload.get("knowledge_points") or []
    resources = payload.get("resources") or []
    relations = payload.get("relations") or []

    stats = {"courses": 0, "knowledge_points": 0, "resources": 0, "relations": 0}

    with driver.session() as session:
        for c in courses:
            name = str(c.get("name", "")).strip()
            if not name:
                continue
            session.run("MERGE (:Course {name: $name})", {"name": name})
            stats["courses"] += 1

        for k in knowledge_points:
            name = str(k.get("name", "")).strip()
            course_name = str(k.get("course", "")).strip()
            if not name:
                continue
            session.run("MERGE (:KnowledgePoint {name: $name})", {"name": name})
            stats["knowledge_points"] += 1
            if course_name:
                session.run(
                    """
                    MERGE (c:Course {name: $course})
                    MERGE (k:KnowledgePoint {name: $kp})
                    MERGE (c)-[:HAS_KP]->(k)
                    """,
                    {"course": course_name, "kp": name},
                )

        for r in resources:
            rid = r.get("id")
            title = str(r.get("title", "")).strip()
            if rid is None or title == "":
                continue
            session.run("MERGE (:Resource {id: $id, title: $title})", {"id": int(rid), "title": title})
            stats["resources"] += 1
            kp = str(r.get("knowledge_point", "")).strip()
            if kp:
                session.run(
                    """
                    MERGE (k:KnowledgePoint {name: $kp})
                    MERGE (r:Resource {id: $id})
                    MERGE (k)-[:RELATED_RESOURCE]->(r)
                    """,
                    {"kp": kp, "id": int(rid)},
                )

        for rel in relations:
            rel_type = str(rel.get("type", "")).strip()
            if rel_type not in {"prerequisite", "related"}:
                continue
            src = str(rel.get("source", "")).strip()
            dst = str(rel.get("target", "")).strip()
            if not src or not dst:
                continue
            edge = "PREREQUISITE" if rel_type == "prerequisite" else "RELATED"
            session.run(
                f"""
                MERGE (a:KnowledgePoint {{name: $src}})
                MERGE (b:KnowledgePoint {{name: $dst}})
                MERGE (a)-[:{edge}]->(b)
                """,
                {"src": src, "dst": dst},
            )
            stats["relations"] += 1

    return stats


def _neo4j_search_resources(driver, knowledge: str) -> List[int]:
    query = """
    MATCH (k:KnowledgePoint {name: $knowledge})
    OPTIONAL MATCH (k)-[:RELATED_RESOURCE]->(r1:Resource)
    OPTIONAL MATCH (k)-[:RELATED|:PREREQUISITE*1..2]-(k2:KnowledgePoint)-[:RELATED_RESOURCE]->(r2:Resource)
    WITH collect(distinct r1.id) + collect(distinct r2.id) as ids
    UNWIND ids as id
    WITH distinct id WHERE id IS NOT NULL
    RETURN id
    """
    with driver.session() as session:
        records = session.run(query, {"knowledge": knowledge})
        return [int(r["id"]) for r in records]


def _recommend(db: Session, user_id: int, neo4j_driver) -> List[Dict[str, Any]]:
    favored = (
        db.execute(
            select(UserBehavior.resource_id)
            .where(UserBehavior.user_id == user_id)
            .where(UserBehavior.action == "favorite")
            .order_by(UserBehavior.created_at.desc())
            .limit(50)
        )
        .scalars()
        .all()
    )
    favored_set = set(favored)

    knowledge_points = (
        db.execute(select(Resource.knowledge_point).where(Resource.id.in_(favored)).where(Resource.knowledge_point.isnot(None)))
        .scalars()
        .all()
    )
    kp_set = {kp for kp in knowledge_points if kp}

    candidates = db.execute(select(Resource).where(Resource.status == "approved")).scalars().all()
    out: List[Tuple[int, str]] = []

    for r in candidates:
        if r.id in favored_set:
            continue
        score = 0
        reasons: List[str] = []
        if r.knowledge_point and r.knowledge_point in kp_set:
            score += 10
            reasons.append(f"因为你学习/收藏了知识点 {r.knowledge_point}")
        tag_set = {t.tag for t in r.tags}
        if tag_set.intersection(kp_set):
            score += 6
            reasons.append("因为资源标签与学习偏好匹配")
        if score > 0:
            out.append((score, json.dumps(reasons, ensure_ascii=False)))

    scored: List[Tuple[int, Resource, List[str]]] = []
    idx = 0
    for r in candidates:
        if r.id in favored_set:
            continue
        score = 0
        reasons: List[str] = []
        if r.knowledge_point and r.knowledge_point in kp_set:
            score += 10
            reasons.append(f"因为你学习了知识点 {r.knowledge_point}，为你推荐关联资源 {r.title}")
        if score == 0 and neo4j_driver and r.knowledge_point:
            related_ids = set(_neo4j_search_resources(neo4j_driver, r.knowledge_point))
            if related_ids.intersection(favored_set):
                score += 4
                reasons.append("因为你收藏了相邻知识点关联资源")
        if score > 0:
            scored.append((score, r, reasons))
        idx += 1

    scored.sort(key=lambda x: (-x[0], -x[1].id))
    items = [{"resource": _resource_dto(r), "reasons": reasons} for score, r, reasons in scored[:20]]
    return items


app = _create_app()


def _print_env_hint() -> None:
    print("Environment variables (optional):")
    print("  DATABASE_URL=mysql+pymysql://user:pass@localhost:3306/teaching_resource?charset=utf8mb4")
    print("  NEO4J_URI=bolt://localhost:7687")
    print("  NEO4J_USER=neo4j")
    print("  NEO4J_PASSWORD=your_password")
    print("  CORS_ORIGINS=http://localhost:5173")
    print("  UPLOAD_DIR=./storage")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("command", nargs="?", default="run", choices=["run", "init-db", "env"])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=5000, type=int)
    args = parser.parse_args()

    if args.command == "env":
        _print_env_hint()
        raise SystemExit(0)

    if args.command == "init-db":
        app.init_db()
        print("OK")
        raise SystemExit(0)

    app.init_db()
    app.run(host=args.host, port=args.port, debug=True)
