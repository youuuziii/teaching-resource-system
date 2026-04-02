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
    inspect,
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
    phone: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
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


class Course(Base):
    __tablename__ = "courses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    code: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())


class KnowledgePoint(Base):
    __tablename__ = "knowledge_points"
    __table_args__ = (UniqueConstraint("name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    course_id: Mapped[Optional[int]] = mapped_column(ForeignKey("courses.id"), nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())

    course: Mapped[Optional[Course]] = relationship("Course")


class Teacher(Base):
    __tablename__ = "teachers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    title: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())


class Student(Base):
    __tablename__ = "students"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True)
    student_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    email: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())


class Dean(Base):
    __tablename__ = "deans"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True)
    dean_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    email: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())


class ResourceTeacher(Base):
    __tablename__ = "resource_teachers"
    __table_args__ = (UniqueConstraint("resource_id", "teacher_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    resource_id: Mapped[int] = mapped_column(ForeignKey("resources.id"), nullable=False)
    teacher_id: Mapped[int] = mapped_column(ForeignKey("teachers.id"), nullable=False)

    teacher: Mapped[Teacher] = relationship("Teacher")
    resource: Mapped["Resource"] = relationship("Resource", back_populates="resource_teachers")

class CourseTeacher(Base):
    __tablename__ = "course_teachers"
    __table_args__ = (UniqueConstraint("course_id", "teacher_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    course_id: Mapped[int] = mapped_column(ForeignKey("courses.id"), nullable=False)
    teacher_id: Mapped[int] = mapped_column(ForeignKey("teachers.id"), nullable=False)

    course: Mapped[Course] = relationship("Course")
    teacher: Mapped[Teacher] = relationship("Teacher")


class Resource(Base):
    __tablename__ = "resources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    file_name: Mapped[str] = mapped_column(String(260), nullable=False)
    file_path: Mapped[str] = mapped_column(String(600), nullable=False)
    file_type: Mapped[str] = mapped_column(String(16), nullable=False)
    file_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    course_id: Mapped[Optional[int]] = mapped_column(ForeignKey("courses.id"), nullable=True)
    knowledge_point_id: Mapped[Optional[int]] = mapped_column(ForeignKey("knowledge_points.id"), nullable=True)
    course_name: Mapped[Optional[str]] = mapped_column("course", String(120), nullable=True)
    knowledge_point_name: Mapped[Optional[str]] = mapped_column("knowledge_point", String(120), nullable=True)
    created_by: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    status: Mapped[str] = mapped_column(
        Enum("pending", "approved", "rejected", name="resource_status"),
        nullable=False,
        default="pending",
    )
    audit_comment: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    audited_by: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True)
    audited_at: Mapped[Optional[dt.datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    course: Mapped[Optional[Course]] = relationship("Course", foreign_keys=[course_id])
    knowledge_point: Mapped[Optional[KnowledgePoint]] = relationship("KnowledgePoint", foreign_keys=[knowledge_point_id])
    tags: Mapped[List["ResourceTag"]] = relationship("ResourceTag", back_populates="resource", cascade="all, delete-orphan")
    resource_teachers: Mapped[List[ResourceTeacher]] = relationship(
        "ResourceTeacher", back_populates="resource", cascade="all, delete-orphan"
    )


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

    def _neo4j_ensure_constraints() -> None:
        if not neo4j_driver:
            return
        with neo4j_driver.session() as session:
            session.run("CREATE CONSTRAINT course_name IF NOT EXISTS FOR (c:Course) REQUIRE c.name IS UNIQUE")
            session.run(
                "CREATE CONSTRAINT knowledge_point_name IF NOT EXISTS FOR (k:KnowledgePoint) REQUIRE k.name IS UNIQUE"
            )
            session.run("CREATE CONSTRAINT teacher_name IF NOT EXISTS FOR (t:Teacher) REQUIRE t.name IS UNIQUE")
            session.run("CREATE CONSTRAINT resource_id IF NOT EXISTS FOR (r:Resource) REQUIRE r.id IS UNIQUE")

    def _neo4j_upsert_course(course: Course) -> None:
        if not neo4j_driver:
            return
        try:
            _neo4j_ensure_constraints()
            with neo4j_driver.session() as session:
                session.run(
                    """
                    MERGE (c:Course {name: $name})
                    SET c.code = $code, c.description = $description
                    """,
                    {"name": course.name, "code": course.code, "description": course.description},
                )
        except Exception:
            return

    def _neo4j_upsert_kp(kp: KnowledgePoint, course: Optional[Course]) -> None:
        if not neo4j_driver:
            return
        try:
            _neo4j_ensure_constraints()
            with neo4j_driver.session() as session:
                session.run("MERGE (k:KnowledgePoint {name: $name})", {"name": kp.name})
                if course and course.name:
                    session.run(
                        """
                        MERGE (c:Course {name: $course})
                        MERGE (k:KnowledgePoint {name: $kp})
                        MERGE (c)-[:HAS_KP]->(k)
                        """,
                        {"course": course.name, "kp": kp.name},
                    )
        except Exception:
            return

    def _neo4j_delete_course(course_name: str) -> None:
        if not neo4j_driver:
            return
        try:
            _neo4j_ensure_constraints()
            with neo4j_driver.session() as session:
                session.run(
                    """
                    MATCH (c:Course {name: $name})
                    OPTIONAL MATCH (c)-[:HAS_KP]->(k:KnowledgePoint)
                    OPTIONAL MATCH (c)-[:HAS_RESOURCE]->(r1:Resource)
                    OPTIONAL MATCH (k)-[:RELATED_RESOURCE]->(r2:Resource)
                    WITH collect(distinct r1) + collect(distinct r2) as rs, collect(distinct k) as ks, c
                    FOREACH (r IN rs | DETACH DELETE r)
                    FOREACH (k IN ks | DETACH DELETE k)
                    DETACH DELETE c
                    """,
                    {"name": course_name},
                )
        except Exception:
            return

    def init_db() -> None:
        Base.metadata.create_all(engine)
        _ensure_schema(engine)
        with SessionLocal() as db:
            _seed_rbac(db)
            _backfill_resource_refs(db)
            users = db.execute(select(User)).scalars().all()
            for u in users:
                _ensure_user_profiles(db, u, [r.name for r in (u.roles or [])])
            db.commit()

    def _ensure_schema(engine_) -> None:
        insp = inspect(engine_)
        if "resources" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("resources")}
            to_add: List[Tuple[str, str]] = []
            if "course_id" not in cols:
                to_add.append(("course_id", "INTEGER"))
            if "knowledge_point_id" not in cols:
                to_add.append(("knowledge_point_id", "INTEGER"))
            if "file_size" not in cols:
                to_add.append(("file_size", "INTEGER"))
            if "audited_by" not in cols:
                to_add.append(("audited_by", "INTEGER"))
            if "audited_at" not in cols:
                to_add.append(("audited_at", "DATETIME"))
            if to_add:
                with engine_.connect() as conn:
                    for name, sql_type in to_add:
                        conn.exec_driver_sql(f"ALTER TABLE resources ADD COLUMN {name} {sql_type}")
                    conn.commit()

        if "users" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("users")}
            to_add: List[Tuple[str, str]] = []
            if "phone" not in cols:
                to_add.append(("phone", "VARCHAR(32)"))
            if to_add:
                with engine_.connect() as conn:
                    for name, sql_type in to_add:
                        conn.exec_driver_sql(f"ALTER TABLE users ADD COLUMN {name} {sql_type}")
                    conn.commit()

        if "teachers" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("teachers")}
            to_add: List[Tuple[str, str]] = []
            if "user_id" not in cols:
                to_add.append(("user_id", "INTEGER"))
            if "title" not in cols:
                to_add.append(("title", "VARCHAR(120)"))
            if to_add:
                with engine_.connect() as conn:
                    for name, sql_type in to_add:
                        conn.exec_driver_sql(f"ALTER TABLE teachers ADD COLUMN {name} {sql_type}")
                    conn.commit()

        if "resource_teachers" not in insp.get_table_names():
            Base.metadata.tables["resource_teachers"].create(engine_, checkfirst=True)
        if "students" not in insp.get_table_names():
            Base.metadata.tables["students"].create(engine_, checkfirst=True)
        if "deans" not in insp.get_table_names():
            Base.metadata.tables["deans"].create(engine_, checkfirst=True)
        if "course_teachers" not in insp.get_table_names():
            Base.metadata.tables["course_teachers"].create(engine_, checkfirst=True)

        if "students" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("students")}
            to_add: List[Tuple[str, str]] = []
            if "user_id" not in cols:
                to_add.append(("user_id", "INTEGER"))
            if to_add:
                with engine_.connect() as conn:
                    for name, sql_type in to_add:
                        conn.exec_driver_sql(f"ALTER TABLE students ADD COLUMN {name} {sql_type}")
                    conn.commit()

        if "deans" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("deans")}
            to_add: List[Tuple[str, str]] = []
            if "user_id" not in cols:
                to_add.append(("user_id", "INTEGER"))
            if to_add:
                with engine_.connect() as conn:
                    for name, sql_type in to_add:
                        conn.exec_driver_sql(f"ALTER TABLE deans ADD COLUMN {name} {sql_type}")
                    conn.commit()

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

    def _parse_int(v: Optional[str]) -> Optional[int]:
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        try:
            return int(s)
        except Exception:
            return None

    def _resolve_course(db: Session, course_id: Optional[int], course_name: Optional[str]) -> Optional[Course]:
        if course_id is not None:
            return db.get(Course, course_id)
        if course_name:
            name = course_name.strip()
            if name:
                existing = db.execute(select(Course).where(Course.name == name)).scalar_one_or_none()
                if existing:
                    return existing
                c = Course(name=name)
                db.add(c)
                db.flush()
                return c
        return None

    def _resolve_kp(
        db: Session, kp_id: Optional[int], kp_name: Optional[str], course: Optional[Course]
    ) -> Optional[KnowledgePoint]:
        if kp_id is not None:
            return db.get(KnowledgePoint, kp_id)
        if kp_name:
            name = kp_name.strip()
            if name:
                existing = db.execute(select(KnowledgePoint).where(KnowledgePoint.name == name)).scalar_one_or_none()
                if existing:
                    if course and existing.course_id is None:
                        existing.course_id = course.id
                        db.flush()
                    return existing
                k = KnowledgePoint(name=name, course_id=course.id if course else None)
                db.add(k)
                db.flush()
                return k
        return None

    def _resolve_teacher(
        db: Session, teacher_id: Optional[int], teacher_name: Optional[str], allow_create: bool
    ) -> Optional[Teacher]:
        if teacher_id is not None:
            return db.get(Teacher, teacher_id)
        if teacher_name:
            name = teacher_name.strip()
            if name:
                existing = db.execute(select(Teacher).where(Teacher.name == name)).scalar_one_or_none()
                if existing:
                    return existing
                if not allow_create:
                    raise ApiError("FORBIDDEN", "Only teacher/admin can create new teachers", 403)
                t = Teacher(name=name)
                db.add(t)
                db.flush()
                return t
        return None

    def _resolve_teachers(
        db: Session, teacher_ids: List[int], teacher_names: List[str], allow_create: bool
    ) -> List[Teacher]:
        out: List[Teacher] = []
        seen: set[int] = set()
        for tid in teacher_ids:
            t = _resolve_teacher(db, tid, None, allow_create=allow_create)
            if t and t.id not in seen:
                seen.add(t.id)
                out.append(t)
        for name in teacher_names:
            t = _resolve_teacher(db, None, name, allow_create=allow_create)
            if t and t.id not in seen:
                seen.add(t.id)
                out.append(t)
        return out

    def _backfill_resource_refs(db: Session) -> None:
        resources = (
            db.execute(
                select(Resource).where(
                    (Resource.course_id.is_(None) & (Resource.course_name.isnot(None)))
                    | (Resource.knowledge_point_id.is_(None) & (Resource.knowledge_point_name.isnot(None)))
                )
            )
            .scalars()
            .all()
        )
        for r in resources:
            course_obj = r.course
            if r.course_id is None and r.course_name:
                course_obj = _resolve_course(db, None, r.course_name)
                if course_obj:
                    r.course_id = course_obj.id
            if r.knowledge_point_id is None and r.knowledge_point_name:
                kp_obj = _resolve_kp(db, None, r.knowledge_point_name, course_obj)
                if kp_obj:
                    r.knowledge_point_id = kp_obj.id

    def seed_demo() -> Dict[str, Any]:
        init_db()
        with SessionLocal() as db:
            def ensure_user(username: str, password: str, role_name: str) -> User:
                user = db.execute(select(User).where(User.username == username)).scalar_one_or_none()
                if not user:
                    user = User(username=username, password_hash=generate_password_hash(password), is_active=True)
                    db.add(user)
                    db.flush()
                role = db.execute(select(Role).where(Role.name == role_name)).scalar_one()
                if role not in user.roles:
                    user.roles.append(role)
                return user

            def ensure_course(name: str) -> Course:
                existing = db.execute(select(Course).where(Course.name == name)).scalar_one_or_none()
                if existing:
                    return existing
                c = Course(name=name)
                db.add(c)
                db.flush()
                return c

            def ensure_kp(name: str, course: Course) -> KnowledgePoint:
                existing = db.execute(select(KnowledgePoint).where(KnowledgePoint.name == name)).scalar_one_or_none()
                if existing:
                    if existing.course_id is None:
                        existing.course_id = course.id
                        db.flush()
                    return existing
                k = KnowledgePoint(name=name, course_id=course.id)
                db.add(k)
                db.flush()
                return k

            def ensure_teacher(name: str, email: Optional[str]) -> Teacher:
                existing = db.execute(select(Teacher).where(Teacher.name == name)).scalar_one_or_none()
                if existing:
                    if email and not existing.email:
                        existing.email = email
                        db.flush()
                    return existing
                t = Teacher(name=name, email=email)
                db.add(t)
                db.flush()
                return t

            def ensure_resource(
                title: str,
                course: Course,
                kp: KnowledgePoint,
                created_by_user: User,
                teachers: List[Teacher],
                tags: List[str],
                status: str,
            ) -> Resource:
                existing = db.execute(select(Resource).where(Resource.title == title)).scalar_one_or_none()
                if existing:
                    if existing.course_id is None:
                        existing.course_id = course.id
                        existing.course_name = course.name
                    if existing.knowledge_point_id is None:
                        existing.knowledge_point_id = kp.id
                        existing.knowledge_point_name = kp.name
                    if existing.status != status:
                        existing.status = status
                    existing.tags.clear()
                    for t in tags:
                        existing.tags.append(ResourceTag(tag=t))
                    existing.resource_teachers.clear()
                    for teacher in teachers:
                        existing.resource_teachers.append(ResourceTeacher(teacher_id=teacher.id))
                    return existing

                ext = ".pdf"
                file_id = str(uuid.uuid4())
                safe_name = f"demo-{file_id}{ext}"
                dest_path = settings.upload_dir / safe_name
                if not dest_path.exists():
                    dest_path.write_bytes(
                        b"%PDF-1.4\n%Demo\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\n"
                    )

                r = Resource(
                    title=title,
                    description="示例数据",
                    file_name=f"{title}.pdf",
                    file_path=str(dest_path),
                    file_type="pdf",
                    file_size=dest_path.stat().st_size,
                    course_id=course.id,
                    knowledge_point_id=kp.id,
                    course_name=course.name,
                    knowledge_point_name=kp.name,
                    created_by=created_by_user.id,
                    status=status,
                )
                db.add(r)
                db.flush()
                for t in tags:
                    r.tags.append(ResourceTag(tag=t))
                for teacher in teachers:
                    r.resource_teachers.append(ResourceTeacher(teacher_id=teacher.id))
                return r

            admin = ensure_user("admin", "admin123", "admin")
            dean = ensure_user("dean", "dean123", "dean")
            teacher_user = ensure_user("teacher", "teacher123", "teacher")
            student = ensure_user("student", "student123", "student")

            c_math = ensure_course("高等数学")
            c_ds = ensure_course("数据结构")

            kp_limit = ensure_kp("极限", c_math)
            kp_derivative = ensure_kp("导数", c_math)
            kp_tree = ensure_kp("二叉树", c_ds)

            t_zhang = ensure_teacher("张老师", "zhang@example.com")
            t_li = ensure_teacher("李老师", "li@example.com")

            # ensure course-teacher assignments
            def ensure_course_teacher(course: Course, teacher: Teacher) -> None:
                exists = db.execute(
                    select(CourseTeacher).where(CourseTeacher.course_id == course.id).where(CourseTeacher.teacher_id == teacher.id)
                ).scalar_one_or_none()
                if not exists:
                    db.add(CourseTeacher(course_id=course.id, teacher_id=teacher.id))

            ensure_course_teacher(c_math, t_zhang)
            ensure_course_teacher(c_ds, t_li)

            r1 = ensure_resource(
                title="极限基础讲义",
                course=c_math,
                kp=kp_limit,
                created_by_user=teacher_user,
                teachers=[t_zhang],
                tags=["讲义", "基础"],
                status="approved",
            )
            r2 = ensure_resource(
                title="导数例题精选",
                course=c_math,
                kp=kp_derivative,
                created_by_user=teacher_user,
                teachers=[t_zhang, t_li],
                tags=["习题", "精选"],
                status="approved",
            )
            r3 = ensure_resource(
                title="二叉树速查表",
                course=c_ds,
                kp=kp_tree,
                created_by_user=teacher_user,
                teachers=[t_li],
                tags=["速查", "算法"],
                status="pending",
            )

            has_fav = (
                db.execute(
                    select(UserBehavior)
                    .where(UserBehavior.user_id == student.id)
                    .where(UserBehavior.resource_id == r1.id)
                    .where(UserBehavior.action == "favorite")
                )
                .scalars()
                .first()
            )
            if not has_fav:
                db.add(UserBehavior(user_id=student.id, resource_id=r1.id, action="favorite"))
            db.commit()
            return {
                "users": [
                    {"username": "admin", "password": "admin123", "role": "admin"},
                    {"username": "dean", "password": "dean123", "role": "dean"},
                    {"username": "teacher", "password": "teacher123", "role": "teacher"},
                    {"username": "student", "password": "student123", "role": "student"},
                ],
                "resources": [
                    {"id": r1.id, "title": r1.title, "status": r1.status},
                    {"id": r2.id, "title": r2.title, "status": r2.status},
                    {"id": r3.id, "title": r3.title, "status": r3.status},
                ],
            }

    def _chunks(seq: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
        for i in range(0, len(seq), size):
            yield seq[i : i + size]

    def sync_neo4j(reset: bool) -> Dict[str, Any]:
        init_db()
        if not neo4j_driver:
            raise ApiError("SERVICE_UNAVAILABLE", "Neo4j not configured", 503)

        with SessionLocal() as db:
            courses = db.execute(select(Course)).scalars().all()
            kps = db.execute(select(KnowledgePoint)).scalars().all()
            teachers = db.execute(select(Teacher)).scalars().all()
            resources = db.execute(select(Resource)).scalars().all()
            links = db.execute(select(ResourceTeacher)).scalars().all()

            course_rows = [
                {"name": c.name, "code": c.code, "description": c.description} for c in courses
            ]
            kp_rows = []
            for k in kps:
                course_name = None
                if k.course_id:
                    c = db.get(Course, k.course_id)
                    course_name = c.name if c else None
                kp_rows.append({"name": k.name, "course_name": course_name})

            teacher_rows = [{"name": t.name, "email": t.email} for t in teachers]

            resource_rows = []
            for r in resources:
                course_label = r.course.name if r.course else r.course_name
                kp_label = r.knowledge_point.name if r.knowledge_point else r.knowledge_point_name
                resource_rows.append(
                    {
                        "id": r.id,
                        "title": r.title,
                        "status": r.status,
                        "course_name": course_label,
                        "knowledge_point": kp_label,
                    }
                )

            authored_rows = []
            for rt in links:
                t = db.get(Teacher, rt.teacher_id)
                if not t:
                    continue
                authored_rows.append({"resource_id": rt.resource_id, "teacher_name": t.name})

        def _ensure_constraints(session) -> None:
            session.run("CREATE CONSTRAINT course_name IF NOT EXISTS FOR (c:Course) REQUIRE c.name IS UNIQUE")
            session.run(
                "CREATE CONSTRAINT knowledge_point_name IF NOT EXISTS FOR (k:KnowledgePoint) REQUIRE k.name IS UNIQUE"
            )
            session.run("CREATE CONSTRAINT teacher_name IF NOT EXISTS FOR (t:Teacher) REQUIRE t.name IS UNIQUE")
            session.run("CREATE CONSTRAINT resource_id IF NOT EXISTS FOR (r:Resource) REQUIRE r.id IS UNIQUE")

        with neo4j_driver.session() as session:
            _ensure_constraints(session)
            if reset:
                session.run("MATCH (n) DETACH DELETE n")
                _ensure_constraints(session)

            for batch in _chunks(course_rows, 500):
                session.run(
                    """
                    UNWIND $rows as row
                    MERGE (c:Course {name: row.name})
                    SET c.code = row.code, c.description = row.description
                    """,
                    {"rows": batch},
                )

            for batch in _chunks(kp_rows, 800):
                session.run(
                    """
                    UNWIND $rows as row
                    MERGE (k:KnowledgePoint {name: row.name})
                    WITH k, row
                    OPTIONAL MATCH (c:Course {name: row.course_name})
                    FOREACH (_ IN CASE WHEN c IS NULL THEN [] ELSE [1] END | MERGE (c)-[:HAS_KP]->(k))
                    """,
                    {"rows": batch},
                )

            for batch in _chunks(teacher_rows, 800):
                session.run(
                    """
                    UNWIND $rows as row
                    MERGE (t:Teacher {name: row.name})
                    SET t.email = row.email
                    """,
                    {"rows": batch},
                )

            for batch in _chunks(resource_rows, 500):
                session.run(
                    """
                    UNWIND $rows as row
                    MERGE (r:Resource {id: row.id})
                    SET r.title = row.title, r.status = row.status
                    WITH r, row
                    OPTIONAL MATCH (c:Course {name: row.course_name})
                    FOREACH (_ IN CASE WHEN c IS NULL THEN [] ELSE [1] END | MERGE (c)-[:HAS_RESOURCE]->(r))
                    WITH r, row
                    OPTIONAL MATCH (k:KnowledgePoint {name: row.knowledge_point})
                    FOREACH (_ IN CASE WHEN k IS NULL THEN [] ELSE [1] END | MERGE (k)-[:RELATED_RESOURCE]->(r))
                    """,
                    {"rows": batch},
                )

            for batch in _chunks(authored_rows, 800):
                session.run(
                    """
                    UNWIND $rows as row
                    MATCH (t:Teacher {name: row.teacher_name})
                    MATCH (r:Resource {id: row.resource_id})
                    MERGE (t)-[:AUTHORED]->(r)
                    """,
                    {"rows": batch},
                )

        stats = {
            "reset": reset,
            "courses": len(course_rows),
            "knowledge_points": len(kp_rows),
            "teachers": len(teacher_rows),
            "resources": len(resource_rows),
            "relations": {
                "has_kp": len([r for r in kp_rows if r.get("course_name")]),
                "has_resource": len([r for r in resource_rows if r.get("course_name")]),
                "related_resource": len([r for r in resource_rows if r.get("knowledge_point")]),
                "authored": len(authored_rows),
            },
        }
        return stats

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

    @app.get("/api/courses")
    def list_courses():
        q = (request.args.get("q") or "").strip()
        with SessionLocal() as db:
            stmt = select(Course).order_by(Course.created_at.desc())
            if q:
                like = f"%{q}%"
                stmt = stmt.where((Course.name.like(like)) | (Course.code.like(like)) | (Course.description.like(like)))
            items = db.execute(stmt).scalars().all()
            return jsonify(
                {
                    "items": [
                        {"id": c.id, "name": c.name, "code": c.code, "description": c.description, "created_at": c.created_at.isoformat()}
                        for c in items
                    ]
                }
            )

    @app.post("/api/courses")
    def create_course():
        data = _json()
        name = str(data.get("name", "")).strip()
        code = (data.get("code") or "").strip() or None
        description = (data.get("description") or "").strip() or None
        if not name:
            raise ApiError("BAD_REQUEST", "name required", 400)
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"dean"})
            existing = db.execute(select(Course).where(Course.name == name)).scalar_one_or_none()
            if existing:
                _neo4j_upsert_course(existing)
                return jsonify({"course": {"id": existing.id, "name": existing.name, "code": existing.code, "description": existing.description}})
            c = Course(name=name, code=code, description=description)
            db.add(c)
            db.commit()
            db.refresh(c)
            _neo4j_upsert_course(c)
            return jsonify({"course": {"id": c.id, "name": c.name, "code": c.code, "description": c.description}})
    
    @app.delete("/api/courses/<int:course_id>")
    def delete_course(course_id: int):
        force_raw = (request.args.get("force") or "").strip().lower()
        force = force_raw in {"1", "true", "yes", "y", "on"}
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"dean"})
            c = db.get(Course, course_id)
            if not c:
                raise ApiError("NOT_FOUND", "course not found", 404)
            kp_used = db.execute(select(func.count(KnowledgePoint.id)).where(KnowledgePoint.course_id == c.id)).scalar_one() or 0
            res_used = (
                db.execute(
                    select(func.count(Resource.id)).where(
                        (Resource.course_id == c.id) | (Resource.course_name == c.name)
                    )
                ).scalar_one()
                or 0
            )
            if (int(kp_used) > 0 or int(res_used) > 0) and not force:
                raise ApiError("CONFLICT", "course has knowledge points or resources", 409)

            course_teacher_rows = db.execute(select(CourseTeacher).where(CourseTeacher.course_id == c.id)).scalars().all()
            for r in course_teacher_rows:
                db.delete(r)

            resource_rows = (
                db.execute(
                    select(Resource).where(
                        (Resource.course_id == c.id) | (Resource.course_name == c.name)
                    )
                )
                .scalars()
                .all()
            )
            resource_ids = [r.id for r in resource_rows]

            behavior_deleted = 0
            if resource_ids:
                behaviors = db.execute(select(UserBehavior).where(UserBehavior.resource_id.in_(resource_ids))).scalars().all()
                for b in behaviors:
                    db.delete(b)
                behavior_deleted = len(behaviors)

                tags = db.execute(select(ResourceTag).where(ResourceTag.resource_id.in_(resource_ids))).scalars().all()
                for t in tags:
                    db.delete(t)

                rts = db.execute(select(ResourceTeacher).where(ResourceTeacher.resource_id.in_(resource_ids))).scalars().all()
                for rt in rts:
                    db.delete(rt)

            file_paths = [str(r.file_path or "") for r in resource_rows]
            for r in resource_rows:
                db.delete(r)

            kp_rows = db.execute(select(KnowledgePoint).where(KnowledgePoint.course_id == c.id)).scalars().all()
            for k in kp_rows:
                db.delete(k)

            db.delete(c)
            db.commit()

            removed_files = 0
            for fp in file_paths:
                if not fp:
                    continue
                try:
                    p = Path(fp)
                    if p.exists():
                        p.unlink()
                        removed_files += 1
                except Exception:
                    pass

            _neo4j_delete_course(c.name)
            return jsonify(
                {
                    "ok": True,
                    "deleted": {
                        "course": 1,
                        "course_teachers": len(course_teacher_rows),
                        "knowledge_points": len(kp_rows),
                        "resources": len(resource_rows),
                        "resource_files": removed_files,
                        "behaviors": behavior_deleted,
                    },
                }
            )
    
    @app.get("/api/courses/<int:course_id>/teachers")
    def course_teachers(course_id: int):
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"dean", "admin"})
            c = db.get(Course, course_id)
            if not c:
                raise ApiError("NOT_FOUND", "course not found", 404)
            rows = db.execute(select(CourseTeacher).where(CourseTeacher.course_id == course_id)).scalars().all()
            items = []
            for ct in rows:
                t = db.get(Teacher, ct.teacher_id)
                if t:
                    items.append({"id": t.id, "name": t.name, "email": t.email})
            return jsonify({"items": items})

    @app.put("/api/courses/<int:course_id>/teachers")
    def set_course_teachers(course_id: int):
        data = _json()
        teacher_ids = data.get("teacher_ids") or []
        if not isinstance(teacher_ids, list) or not all(isinstance(t, int) for t in teacher_ids):
            raise ApiError("BAD_REQUEST", "teacher_ids must be int[]", 400)
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"dean"})
            c = db.get(Course, course_id)
            if not c:
                raise ApiError("NOT_FOUND", "course not found", 404)
            existing = db.execute(select(CourseTeacher).where(CourseTeacher.course_id == course_id)).scalars().all()
            keep_ids = set(int(tid) for tid in teacher_ids)
            # delete removed
            for row in existing:
                if row.teacher_id not in keep_ids:
                    db.delete(row)
            # add missing
            current_ids = {row.teacher_id for row in existing}
            for tid in keep_ids.difference(current_ids):
                t = db.get(Teacher, tid)
                if not t:
                    continue
                db.add(CourseTeacher(course_id=course_id, teacher_id=tid))
            db.commit()
            return jsonify({"ok": True})

    @app.get("/api/knowledge-points")
    def list_knowledge_points():
        course_id = _parse_int(request.args.get("course_id"))
        keyword = (request.args.get("keyword") or "").strip()
        with SessionLocal() as db:
            q = select(KnowledgePoint).order_by(KnowledgePoint.created_at.desc())
            if course_id is not None:
                q = q.where(KnowledgePoint.course_id == course_id)
            if keyword:
                like = f"%{keyword}%"
                q = q.where(KnowledgePoint.name.like(like))
            items = db.execute(q).scalars().all()
            return jsonify({"items": [{"id": k.id, "name": k.name, "course_id": k.course_id} for k in items]})

    @app.post("/api/knowledge-points")
    def create_knowledge_point():
        data = _json()
        name = str(data.get("name", "")).strip()
        course_id = data.get("course_id")
        course_name = (data.get("course_name") or "").strip() or None
        if not name:
            raise ApiError("BAD_REQUEST", "name required", 400)
        with SessionLocal() as db:
            user = require_auth(db)
            roles = {r.name for r in user.roles}
            if "dean" in roles:
                c = _resolve_course(db, int(course_id) if isinstance(course_id, int) else _parse_int(course_id), course_name)
            else:
                require_roles(user, {"teacher"})
                # teacher cannot create course implicitly; must specify an existing course they are assigned to
                if isinstance(course_id, int):
                    cid = course_id
                else:
                    cid = _parse_int(course_id)
                if cid is None:
                    raise ApiError("FORBIDDEN", "teacher must specify an assigned course", 403)
                c = db.get(Course, cid)
                if not c:
                    raise ApiError("NOT_FOUND", "course not found", 404)
                # ensure assignment
                trow = db.execute(select(Teacher).where(Teacher.user_id == user.id)).scalar_one_or_none()
                if not trow:
                    raise ApiError("FORBIDDEN", "teacher profile not found", 403)
                assigned = (
                    db.execute(
                        select(CourseTeacher).where(CourseTeacher.course_id == c.id).where(CourseTeacher.teacher_id == trow.id)
                    )
                    .scalars()
                    .first()
                )
                if not assigned:
                    raise ApiError("FORBIDDEN", "not assigned to this course", 403)
            existing = db.execute(select(KnowledgePoint).where(KnowledgePoint.name == name)).scalar_one_or_none()
            if existing:
                if c and existing.course_id is None:
                    existing.course_id = c.id
                    db.commit()
                if c:
                    _neo4j_upsert_course(c)
                _neo4j_upsert_kp(existing, c)
                return jsonify({"knowledge_point": {"id": existing.id, "name": existing.name, "course_id": existing.course_id}})
            k = KnowledgePoint(name=name, course_id=c.id if c else None)
            db.add(k)
            db.commit()
            db.refresh(k)
            if c:
                _neo4j_upsert_course(c)
            _neo4j_upsert_kp(k, c)
            return jsonify({"knowledge_point": {"id": k.id, "name": k.name, "course_id": k.course_id}})

    @app.patch("/api/knowledge-points/<int:kp_id>")
    def update_knowledge_point(kp_id: int):
        data = _json()
        name = data.get("name")
        if name is None or not isinstance(name, str):
            raise ApiError("BAD_REQUEST", "name required", 400)
        name = name.strip()
        if not name:
            raise ApiError("BAD_REQUEST", "name required", 400)
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"teacher"})
            kp = db.get(KnowledgePoint, kp_id)
            if not kp:
                raise ApiError("NOT_FOUND", "knowledge point not found", 404)
            if not kp.course_id:
                raise ApiError("FORBIDDEN", "knowledge point has no course", 403)
            trow = db.execute(select(Teacher).where(Teacher.user_id == user.id)).scalar_one_or_none()
            if not trow:
                raise ApiError("FORBIDDEN", "teacher profile not found", 403)
            assigned = (
                db.execute(
                    select(CourseTeacher)
                    .where(CourseTeacher.course_id == kp.course_id)
                    .where(CourseTeacher.teacher_id == trow.id)
                )
                .scalars()
                .first()
            )
            if not assigned:
                raise ApiError("FORBIDDEN", "not assigned to this course", 403)
            kp.name = name
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
                raise ApiError("CONFLICT", "knowledge point already exists", 409)
            db.refresh(kp)
            return jsonify({"knowledge_point": {"id": kp.id, "name": kp.name, "course_id": kp.course_id}})

    @app.delete("/api/knowledge-points/<int:kp_id>")
    def delete_knowledge_point(kp_id: int):
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"teacher"})
            kp = db.get(KnowledgePoint, kp_id)
            if not kp:
                raise ApiError("NOT_FOUND", "knowledge point not found", 404)
            if not kp.course_id:
                raise ApiError("FORBIDDEN", "knowledge point has no course", 403)
            trow = db.execute(select(Teacher).where(Teacher.user_id == user.id)).scalar_one_or_none()
            if not trow:
                raise ApiError("FORBIDDEN", "teacher profile not found", 403)
            assigned = (
                db.execute(
                    select(CourseTeacher)
                    .where(CourseTeacher.course_id == kp.course_id)
                    .where(CourseTeacher.teacher_id == trow.id)
                )
                .scalars()
                .first()
            )
            if not assigned:
                raise ApiError("FORBIDDEN", "not assigned to this course", 403)
            used = (
                db.execute(select(func.count(Resource.id)).where(Resource.knowledge_point_id == kp.id))
                .scalar_one()
                or 0
            )
            if int(used) > 0:
                raise ApiError("CONFLICT", "knowledge point is used by resources", 409)
            db.delete(kp)
            db.commit()
            return jsonify({"ok": True})

    @app.get("/api/teachers")
    def list_teachers():
        keyword = (request.args.get("keyword") or "").strip()
        with SessionLocal() as db:
            q = select(Teacher).order_by(Teacher.created_at.desc())
            if keyword:
                like = f"%{keyword}%"
                q = q.where(Teacher.name.like(like))
            items = db.execute(q).scalars().all()
            return jsonify(
                {"items": [{"id": t.id, "name": t.name, "email": t.email, "created_at": t.created_at.isoformat()} for t in items]}
            )

    @app.post("/api/teachers")
    def create_teacher():
        data = _json()
        name = str(data.get("name", "")).strip()
        email = (data.get("email") or "").strip() or None
        if not name:
            raise ApiError("BAD_REQUEST", "name required", 400)
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"teacher", "dean"})
            existing = db.execute(select(Teacher).where(Teacher.name == name)).scalar_one_or_none()
            if existing:
                return jsonify({"teacher": {"id": existing.id, "name": existing.name, "email": existing.email}})
            t = Teacher(name=name, email=email)
            db.add(t)
            db.commit()
            db.refresh(t)
            return jsonify({"teacher": {"id": t.id, "name": t.name, "email": t.email}})

    @app.post("/api/auth/register")
    def register():
        data = _json()
        username = str(data.get("username", "")).strip()
        password = str(data.get("password", "")).strip()
        role = "student"
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
            db.flush()
            _ensure_user_profiles(db, new_user, ["student"])
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

    @app.put("/api/me")
    def update_me():
        data = _json()
        phone = data.get("phone")
        password = data.get("password")

        if phone is not None and not isinstance(phone, str):
            raise ApiError("BAD_REQUEST", "phone must be string", 400)
        if password is not None and not isinstance(password, str):
            raise ApiError("BAD_REQUEST", "password must be string", 400)

        with SessionLocal() as db:
            user = require_auth(db)

            if phone is not None:
                user.phone = phone
            if password is not None and password.strip():
                user.password_hash = generate_password_hash(password.strip())

            db.commit()
            db.refresh(user)
            return jsonify({"user": _user_dto(user)})

    @app.get("/api/me/history")
    def me_history():
        limit = _parse_int(request.args.get("limit")) or 50
        limit = max(1, min(limit, 200))
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"student"})
            behaviors = (
                db.execute(
                    select(UserBehavior)
                    .where(UserBehavior.user_id == user.id)
                    .where(UserBehavior.action == "view")
                    .order_by(UserBehavior.created_at.desc())
                    .limit(limit)
                )
                .scalars()
                .all()
            )
            resource_ids = [b.resource_id for b in behaviors]
            if not resource_ids:
                return jsonify({"items": []})
            resources = db.execute(select(Resource).where(Resource.id.in_(resource_ids))).scalars().all()
            by_id = {r.id: r for r in resources}
            items = []
            for b in behaviors:
                r = by_id.get(b.resource_id)
                if not r:
                    continue
                items.append({"resource": _resource_dto(r), "viewed_at": b.created_at.isoformat()})
            return jsonify({"items": items})

    @app.get("/api/me/favorites")
    def me_favorites():
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"student"})
            behaviors = (
                db.execute(
                    select(UserBehavior)
                    .where(UserBehavior.user_id == user.id)
                    .where(UserBehavior.action.in_(["favorite", "unfavorite"]))
                    .order_by(UserBehavior.created_at.desc())
                    .limit(1000)
                )
                .scalars()
                .all()
            )
            latest: Dict[int, UserBehavior] = {}
            for b in behaviors:
                if b.resource_id not in latest:
                    latest[b.resource_id] = b
            favorite_ids = [rid for rid, b in latest.items() if b.action == "favorite"]
            if not favorite_ids:
                return jsonify({"items": []})
            resources = (
                db.execute(select(Resource).where(Resource.status == "approved").where(Resource.id.in_(favorite_ids)))
                .scalars()
                .all()
            )
            by_id = {r.id: r for r in resources}
            items = []
            for rid in favorite_ids:
                r = by_id.get(rid)
                if not r:
                    continue
                items.append(_resource_dto(r))
            return jsonify({"items": items})

    @app.get("/api/me/courses")
    def me_courses():
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"teacher"})
            trow = db.execute(select(Teacher).where(Teacher.user_id == user.id)).scalar_one_or_none()
            if not trow:
                raise ApiError("FORBIDDEN", "teacher profile not found", 403)
            course_ids = (
                db.execute(select(CourseTeacher.course_id).where(CourseTeacher.teacher_id == trow.id))
                .scalars()
                .all()
            )
            if not course_ids:
                return jsonify({"items": []})
            rows = db.execute(select(Course).where(Course.id.in_(course_ids))).scalars().all()
            by_id = {c.id: c for c in rows}
            ordered = [by_id[cid] for cid in course_ids if cid in by_id]
            return jsonify(
                {
                    "items": [
                        {
                            "id": c.id,
                            "name": c.name,
                            "code": c.code,
                            "description": c.description,
                            "created_at": c.created_at.isoformat() if c.created_at else None,
                        }
                        for c in ordered
                    ]
                }
            )

    @app.get("/api/me/resources")
    def me_resources():
        status = (request.args.get("status") or "").strip().lower()
        course_id = _parse_int(request.args.get("course_id"))
        knowledge_point_id = _parse_int(request.args.get("knowledge_point_id"))
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"teacher"})
            q = select(Resource).where(Resource.created_by == user.id).order_by(Resource.created_at.desc())
            if status and status != "all":
                if status not in {"pending", "approved", "rejected"}:
                    raise ApiError("BAD_REQUEST", "invalid status", 400)
                q = q.where(Resource.status == status)
            if course_id is not None:
                q = q.where(Resource.course_id == course_id)
            if knowledge_point_id is not None:
                q = q.where(Resource.knowledge_point_id == knowledge_point_id)
            rows = db.execute(q).scalars().all()
            return jsonify({"items": [_resource_dto(r) for r in rows]})

    @app.post("/api/resources/upload")
    def upload_resource():
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"teacher"})

            if "file" not in request.files:
                raise ApiError("BAD_REQUEST", "file is required", 400)
            f = request.files["file"]
            title = (request.form.get("title") or f.filename or "").strip()
            description = (request.form.get("description") or "").strip() or None
            course_id = _parse_int(request.form.get("course_id"))
            knowledge_point_id = _parse_int(request.form.get("knowledge_point_id"))
            tags_raw = (request.form.get("tags") or "").strip()

            if not title:
                raise ApiError("BAD_REQUEST", "title is required", 400)
            if course_id is None or knowledge_point_id is None:
                raise ApiError("BAD_REQUEST", "course_id and knowledge_point_id are required", 400)

            original_name = (f.filename or "upload").strip()
            suffix = Path(original_name).suffix.lower()
            if suffix not in {".pdf", ".doc", ".docx"}:
                raise ApiError("BAD_REQUEST", "Only PDF/Word supported", 400)

            file_id = str(uuid.uuid4())
            safe_name = f"{file_id}{suffix}"
            dest_path = settings.upload_dir / safe_name
            f.save(dest_path)

            course_obj = db.get(Course, course_id)
            if not course_obj:
                raise ApiError("NOT_FOUND", "course not found", 404)
            kp_obj = db.get(KnowledgePoint, knowledge_point_id)
            if not kp_obj:
                raise ApiError("NOT_FOUND", "knowledge point not found", 404)
            if kp_obj.course_id != course_obj.id:
                raise ApiError("FORBIDDEN", "knowledge point does not belong to this course", 403)

            trow = db.execute(select(Teacher).where(Teacher.user_id == user.id)).scalar_one_or_none()
            if not trow:
                raise ApiError("FORBIDDEN", "teacher profile not found", 403)
            assigned = (
                db.execute(
                    select(CourseTeacher)
                    .where(CourseTeacher.course_id == course_obj.id)
                    .where(CourseTeacher.teacher_id == trow.id)
                )
                .scalars()
                .first()
            )
            if not assigned:
                raise ApiError("FORBIDDEN", "not assigned to this course", 403)

            res = Resource(
                title=title,
                description=description,
                file_name=original_name,
                file_path=str(dest_path),
                file_type=suffix.lstrip("."),
                file_size=dest_path.stat().st_size if dest_path.exists() else None,
                course_id=course_obj.id if course_obj else None,
                knowledge_point_id=kp_obj.id if kp_obj else None,
                course_name=course_obj.name if course_obj else None,
                knowledge_point_name=kp_obj.name if kp_obj else None,
                created_by=user.id,
                status="pending",
            )
            db.add(res)
            db.flush()

            tags = _split_tags(tags_raw)
            for t in tags:
                res.tags.append(ResourceTag(tag=t))
            res.resource_teachers.append(ResourceTeacher(teacher_id=trow.id))
            db.commit()
            db.refresh(res)
            return jsonify({"resource": _resource_dto(res)})

    @app.get("/api/resources")
    def list_resources():
        status = (request.args.get("status") or "").strip()
        keyword = (request.args.get("keyword") or "").strip()
        tag = (request.args.get("tag") or "").strip()
        course_id = _parse_int(request.args.get("course_id"))
        knowledge_point_id = _parse_int(request.args.get("knowledge_point_id"))
        teacher_id = _parse_int(request.args.get("teacher_id"))

        with SessionLocal() as db:
            if status not in {"pending", "approved", "rejected"}:
                status = "approved"
            current = get_current_user(db)
            user: Optional[User] = None
            if status in {"pending", "rejected"}:
                user = require_auth(db)
                user_roles = {r.name for r in user.roles}
                if status == "pending":
                    if not user_roles.intersection({"dean", "teacher"}):
                        raise ApiError("FORBIDDEN", "Insufficient role", 403)
                else:
                    if not user_roles.intersection({"dean", "admin", "teacher"}):
                        raise ApiError("FORBIDDEN", "Insufficient role", 403)

            q = select(Resource).order_by(Resource.created_at.desc())
            q = q.where(Resource.status == status)
            if user and status in {"pending", "rejected"}:
                user_roles = {r.name for r in user.roles}
                if "teacher" in user_roles and "dean" not in user_roles and "admin" not in user_roles:
                    q = q.where(Resource.created_by == user.id)
            if status == "approved" and current:
                roles = {r.name for r in current.roles}
                if "teacher" in roles and "dean" not in roles and "admin" not in roles:
                    q = q.where(Resource.created_by == current.id)
            if keyword:
                like = f"%{keyword}%"
                q = q.where((Resource.title.like(like)) | (Resource.description.like(like)))
            if course_id is not None:
                q = q.where(Resource.course_id == course_id)
            if knowledge_point_id is not None:
                q = q.where(Resource.knowledge_point_id == knowledge_point_id)
            if teacher_id is not None:
                q = q.join(ResourceTeacher, ResourceTeacher.resource_id == Resource.id).where(
                    ResourceTeacher.teacher_id == teacher_id
                )
            resources = db.execute(q).scalars().all()
            if tag:
                resources = [r for r in resources if tag in {t.tag for t in r.tags}]
            return jsonify({"items": [_resource_dto(r) for r in resources]})

    @app.get("/api/resources/<int:resource_id>")
    def get_resource(resource_id: int):
        with SessionLocal() as db:
            res = db.get(Resource, resource_id)
            if not res:
                raise ApiError("NOT_FOUND", "resource not found", 404)
            user_opt: Optional[User] = None
            if res.status != "approved":
                user_opt = require_auth(db)
                roles = {r.name for r in user_opt.roles}
                if "dean" in roles:
                    pass
                elif "teacher" in roles and res.created_by == user_opt.id:
                    pass
                elif "admin" in roles and res.status == "rejected":
                    pass
                else:
                    raise ApiError("FORBIDDEN", "resource not approved", 403)
            else:
                # even for approved, teachers cannot view others' resources
                auth_user = get_current_user(db)
                if auth_user:
                    roles = {r.name for r in auth_user.roles}
                    if "teacher" in roles and "dean" not in roles and "admin" not in roles:
                        if res.created_by != auth_user.id:
                            raise ApiError("FORBIDDEN", "teacher cannot view others' resources", 403)

            created_user = db.get(User, res.created_by)
            audited_user = db.get(User, res.audited_by) if res.audited_by else None

            return jsonify(
                {
                    "resource": _resource_dto(res),
                    "created_by_user": _user_dto(created_user) if created_user else None,
                    "audited_by_user": _user_dto(audited_user) if audited_user else None,
                }
            )

    @app.get("/api/resources/<int:resource_id>/download")
    def download_resource(resource_id: int):
        with SessionLocal() as db:
            user = require_auth(db)
            res = db.get(Resource, resource_id)
            if not res:
                raise ApiError("NOT_FOUND", "resource not found", 404)
            if res.status != "approved":
                roles = {r.name for r in user.roles}
                if "dean" in roles:
                    pass
                elif "teacher" in roles and res.created_by == user.id:
                    pass
                else:
                    raise ApiError("FORBIDDEN", "resource not approved", 403)
            else:
                roles = {r.name for r in user.roles}
                if "teacher" in roles and "dean" not in roles and "admin" not in roles:
                    if res.created_by != user.id:
                        raise ApiError("FORBIDDEN", "teacher cannot download others' resources", 403)
            if res.status == "approved":
                db.add(UserBehavior(user_id=user.id, resource_id=res.id, action="view"))
                db.commit()

            p = Path(res.file_path)
            return send_from_directory(p.parent, p.name, as_attachment=True, download_name=res.file_name)

    @app.delete("/api/resources/<int:resource_id>")
    def delete_resource(resource_id: int):
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"teacher"})
            res = db.get(Resource, resource_id)
            if not res:
                raise ApiError("NOT_FOUND", "resource not found", 404)
            if res.created_by != user.id:
                raise ApiError("FORBIDDEN", "cannot delete others' resources", 403)
            p = Path(res.file_path)
            db.delete(res)
            db.commit()
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass
            return jsonify({"ok": True})

    @app.patch("/api/resources/<int:resource_id>/audit")
    def audit_resource(resource_id: int):
        data = _json()
        status = str(data.get("status", "")).strip()
        comment = (data.get("comment") or "").strip() or None
        if status not in {"approved", "rejected"}:
            raise ApiError("BAD_REQUEST", "status must be approved/rejected", 400)

        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"dean"})

            res = db.get(Resource, resource_id)
            if not res:
                raise ApiError("NOT_FOUND", "resource not found", 404)
            if res.status != "pending":
                raise ApiError("FORBIDDEN", "Only pending resources can be audited", 403)
            res.status = status
            res.audit_comment = comment
            res.audited_by = user.id
            res.audited_at = dt.datetime.utcnow()
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
            res = db.get(Resource, resource_id)
            if not res:
                raise ApiError("NOT_FOUND", "resource not found", 404)
            roles = {r.name for r in user.roles}
            if "dean" in roles:
                pass
            elif "teacher" in roles and res.created_by == user.id:
                pass
            else:
                raise ApiError("FORBIDDEN", "Insufficient role", 403)
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
            require_roles(user, {"student"})
            res = db.get(Resource, resource_id)
            if not res:
                raise ApiError("NOT_FOUND", "resource not found", 404)
            db.add(UserBehavior(user_id=user.id, resource_id=resource_id, action=action))
            db.commit()
            return jsonify({"ok": True})

    @app.post("/api/resources/<int:resource_id>/teachers")
    def set_resource_teachers(resource_id: int):
        data = _json()
        teacher_ids = data.get("teacher_ids") or []
        teacher_names = data.get("teacher_names") or data.get("teachers") or []
        if not isinstance(teacher_ids, list) or not all(isinstance(t, int) for t in teacher_ids):
            raise ApiError("BAD_REQUEST", "teacher_ids must be int[]", 400)
        if not isinstance(teacher_names, list) or not all(isinstance(t, str) for t in teacher_names):
            raise ApiError("BAD_REQUEST", "teacher_names must be string[]", 400)

        with SessionLocal() as db:
            user = require_auth(db)
            res = db.get(Resource, resource_id)
            if not res:
                raise ApiError("NOT_FOUND", "resource not found", 404)
            roles = {r.name for r in user.roles}
            if "dean" in roles:
                allow_create = True
            elif "teacher" in roles and res.created_by == user.id:
                allow_create = True
            else:
                raise ApiError("FORBIDDEN", "Insufficient role", 403)

            teachers = _resolve_teachers(
                db, teacher_ids=teacher_ids, teacher_names=_split_names(",".join(teacher_names)), allow_create=allow_create
            )
            res.resource_teachers.clear()
            for t in teachers:
                res.resource_teachers.append(ResourceTeacher(teacher_id=t.id))
            db.commit()
            db.refresh(res)
            return jsonify({"resource": _resource_dto(res)})

    @app.get("/api/search")
    def semantic_search():
        keyword = (request.args.get("keyword") or "").strip()
        knowledge = (request.args.get("knowledge") or "").strip()
        course_id = _parse_int(request.args.get("course_id"))
        knowledge_point_id = _parse_int(request.args.get("knowledge_point_id"))

        with SessionLocal() as db:
            current = get_current_user(db)
            q = select(Resource).where(Resource.status == "approved").order_by(Resource.created_at.desc())
            if current:
                roles = {r.name for r in current.roles}
                if "teacher" in roles and "dean" not in roles and "admin" not in roles:
                    q = q.where(Resource.created_by == current.id)
            if keyword:
                like = f"%{keyword}%"
                q = q.where((Resource.title.like(like)) | (Resource.description.like(like)))
            if course_id is not None:
                q = q.where(Resource.course_id == course_id)
            if knowledge_point_id is not None:
                q = q.where(Resource.knowledge_point_id == knowledge_point_id)
            items = db.execute(q).scalars().all()

            related_resource_ids: Optional[set[int]] = None
            paths_by_id: Dict[int, List[str]] = {}
            neo4j_error = False
            if knowledge and neo4j_driver:
                try:
                    paths_by_id = _neo4j_search_resource_paths(neo4j_driver, knowledge)
                    related_resource_ids = set(paths_by_id.keys())
                    if not related_resource_ids:
                        related_resource_ids = None
                except Exception:
                    neo4j_error = True
                    related_resource_ids = None
                    paths_by_id = {}

            if related_resource_ids is not None:
                items = [r for r in items if r.id in related_resource_ids]
            elif knowledge:
                items = [
                    r
                    for r in items
                    if (r.knowledge_point and r.knowledge_point.name == knowledge)
                    or (r.knowledge_point_name == knowledge)
                ]

            out_items = []
            for r in items:
                reasons: List[str] = []
                if keyword:
                    kw = keyword.lower()
                    if (r.title and kw in r.title.lower()) or (r.description and kw in r.description.lower()):
                        reasons.append("关键词命中资源标题/描述")
                if knowledge:
                    kp_name = r.knowledge_point.name if r.knowledge_point else r.knowledge_point_name
                    if kp_name == knowledge:
                        reasons.append("知识点精确匹配")
                    elif related_resource_ids is not None:
                        reasons.append("图谱路径关联匹配")
                if course_id is not None:
                    reasons.append("课程过滤匹配")
                if knowledge_point_id is not None:
                    reasons.append("知识点ID过滤匹配")
                out_items.append(
                    {
                        "resource": _resource_dto(r),
                        "reasons": reasons,
                        "paths": paths_by_id.get(r.id, []),
                    }
                )

            return jsonify({"items": out_items})

    @app.get("/api/recommendations")
    def recommendations():
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"student"})
            items = _recommend(db, user_id=user.id, neo4j_driver=neo4j_driver)
            return jsonify({"items": items})

    @app.get("/api/graph/overview")
    def graph_overview():
        course = (request.args.get("course") or "").strip() or None
        level = (request.args.get("level") or "").strip().lower() or "full"
        if level not in {"full", "courses"}:
            level = "full"
        with SessionLocal() as db:
            current = get_current_user(db)
            if neo4j_driver:
                if current:
                    roles = {r.name for r in current.roles}
                    if not ("teacher" in roles and "dean" not in roles and "admin" not in roles):
                        return jsonify(_neo4j_overview(neo4j_driver, course=course, level=level))
                else:
                    return jsonify(_neo4j_overview(neo4j_driver, course=course, level=level))

            if level == "courses":
                nodes: Dict[str, Dict[str, Any]] = {}

                def upsert_node(node_id: str, label: str, ntype: str) -> None:
                    nodes.setdefault(node_id, {"id": node_id, "label": label, "type": ntype})

                q = select(Course).order_by(Course.created_at.desc())
                if course:
                    q = q.where(Course.name == course)
                rows = db.execute(q).scalars().all()
                if rows:
                    for c in rows:
                        if c.name:
                            upsert_node(f"course:{c.name}", c.name, "course")
                else:
                    q2 = select(Resource.course_name).where(Resource.status == "approved").order_by(Resource.created_at.desc())
                    if course:
                        q2 = q2.where(Resource.course_name == course)
                    if current:
                        roles = {r.name for r in current.roles}
                        if "teacher" in roles and "dean" not in roles and "admin" not in roles:
                            q2 = q2.where(Resource.created_by == current.id)
                    for name in db.execute(q2).scalars().all():
                        if name:
                            upsert_node(f"course:{name}", name, "course")

                return jsonify({"nodes": list(nodes.values()), "links": [], "source": "mysql"})

            q = select(Resource).where(Resource.status == "approved").order_by(Resource.created_at.desc())
            if course:
                q = q.where((Resource.course_name == course))
            if current:
                roles = {r.name for r in current.roles}
                if "teacher" in roles and "dean" not in roles and "admin" not in roles:
                    q = q.where(Resource.created_by == current.id)
            resources = db.execute(q).scalars().all()

            nodes: Dict[str, Dict[str, Any]] = {}
            links: List[Dict[str, Any]] = []

            def upsert_node(node_id: str, label: str, ntype: str) -> None:
                nodes.setdefault(node_id, {"id": node_id, "label": label, "type": ntype})

            for r in resources:
                course_label = r.course.name if r.course else r.course_name
                kp_label = r.knowledge_point.name if r.knowledge_point else r.knowledge_point_name
                if course_label:
                    upsert_node(f"course:{course_label}", course_label, "course")
                if kp_label:
                    upsert_node(f"kp:{kp_label}", kp_label, "knowledge_point")
                upsert_node(f"res:{r.id}", r.title, "resource")

                if course_label:
                    links.append({"source": f"course:{course_label}", "target": f"res:{r.id}", "type": "has"})
                if kp_label:
                    links.append({"source": f"kp:{kp_label}", "target": f"res:{r.id}", "type": "related"})

            return jsonify({"nodes": list(nodes.values()), "links": links, "source": "mysql"})

    @app.get("/api/graph/resources")
    def graph_resources():
        node_id = (request.args.get("node_id") or "").strip()
        if not node_id:
            raise ApiError("BAD_REQUEST", "node_id required", 400)

        def parse_node_id(v: str) -> Tuple[str, str]:
            if ":" not in v:
                return ("unknown", v)
            t, raw = v.split(":", 1)
            return (t.strip(), raw.strip())

        node_type, raw = parse_node_id(node_id)
        if not raw:
            raise ApiError("BAD_REQUEST", "invalid node_id", 400)

        with SessionLocal() as db:
            resource_ids: List[int] = []
            via = "mysql"

            if neo4j_driver:
                via = "neo4j"
                try:
                    if node_type == "kp":
                        query = """
                        MATCH (k:KnowledgePoint {name: $name})-[:RELATED_RESOURCE]->(r:Resource)
                        RETURN DISTINCT r.id AS id
                        """
                        with neo4j_driver.session() as session:
                            rows = session.run(query, {"name": raw}).data()
                        resource_ids = [int(r["id"]) for r in rows if r.get("id") is not None]
                    elif node_type == "course":
                        query = """
                        MATCH (c:Course {name: $name})-[:HAS_RESOURCE]->(r:Resource)
                        RETURN DISTINCT r.id AS id
                        UNION
                        MATCH (c:Course {name: $name})-[:HAS_KP]->(k:KnowledgePoint)-[:RELATED_RESOURCE]->(r:Resource)
                        RETURN DISTINCT r.id AS id
                        """
                        with neo4j_driver.session() as session:
                            rows = session.run(query, {"name": raw}).data()
                        resource_ids = [int(r["id"]) for r in rows if r.get("id") is not None]
                    elif node_type == "teacher":
                        query = """
                        MATCH (t:Teacher {name: $name})-[:AUTHORED]->(r:Resource)
                        RETURN DISTINCT r.id AS id
                        """
                        with neo4j_driver.session() as session:
                            rows = session.run(query, {"name": raw}).data()
                        resource_ids = [int(r["id"]) for r in rows if r.get("id") is not None]
                    elif node_type == "res":
                        try:
                            rid = int(raw)
                            resource_ids = [rid]
                        except Exception:
                            resource_ids = []
                    else:
                        resource_ids = []
                except Exception:
                    via = "mysql"
                    resource_ids = []

            if via == "mysql":
                if node_type == "kp":
                    kp = db.execute(select(KnowledgePoint).where(KnowledgePoint.name == raw)).scalar_one_or_none()
                    if kp:
                        resource_ids = (
                            db.execute(
                                select(Resource.id).where(
                                    (Resource.status == "approved")
                                    & ((Resource.knowledge_point_id == kp.id) | (Resource.knowledge_point_name == raw))
                                )
                            )
                            .scalars()
                            .all()
                        )
                    else:
                        resource_ids = (
                            db.execute(
                                select(Resource.id).where(
                                    (Resource.status == "approved") & (Resource.knowledge_point_name == raw)
                                )
                            )
                            .scalars()
                            .all()
                        )
                elif node_type == "course":
                    c = db.execute(select(Course).where(Course.name == raw)).scalar_one_or_none()
                    if c:
                        resource_ids = (
                            db.execute(
                                select(Resource.id).where(
                                    (Resource.status == "approved")
                                    & ((Resource.course_id == c.id) | (Resource.course_name == raw))
                                )
                            )
                            .scalars()
                            .all()
                        )
                    else:
                        resource_ids = (
                            db.execute(select(Resource.id).where((Resource.status == "approved") & (Resource.course_name == raw)))
                            .scalars()
                            .all()
                        )
                elif node_type == "teacher":
                    t = db.execute(select(Teacher).where(Teacher.name == raw)).scalar_one_or_none()
                    if t:
                        resource_ids = (
                            db.execute(
                                select(ResourceTeacher.resource_id).where(ResourceTeacher.teacher_id == t.id)
                            )
                            .scalars()
                            .all()
                        )
                    else:
                        resource_ids = []
                elif node_type == "res":
                    try:
                        resource_ids = [int(raw)]
                    except Exception:
                        resource_ids = []

            if not resource_ids:
                return jsonify({"ok": True, "via": via, "items": []})

            stmt = select(Resource).where(Resource.status == "approved").where(Resource.id.in_(resource_ids))
            current = get_current_user(db)
            if current:
                roles = {r.name for r in current.roles}
                if "teacher" in roles and "dean" not in roles and "admin" not in roles:
                    stmt = stmt.where(Resource.created_by == current.id)
            resources = db.execute(stmt).scalars().all()
            by_id = {r.id: r for r in resources}
            ordered = [by_id[rid] for rid in resource_ids if rid in by_id]
            return jsonify({"ok": True, "via": via, "items": [_resource_dto(r) for r in ordered]})

    @app.get("/api/graph/explore")
    def graph_explore():
        node_id = (request.args.get("node_id") or "").strip()
        if not node_id:
            raise ApiError("BAD_REQUEST", "node_id required", 400)

        depth_raw = _parse_int(request.args.get("depth"))
        depth = depth_raw if depth_raw in {1, 2} else 2
        expand = (request.args.get("expand") or "").strip().lower() or "full"
        if expand not in {"full", "kps", "resources"}:
            expand = "full"

        def parse_node_id(v: str) -> Tuple[str, str]:
            if ":" not in v:
                return ("unknown", v)
            t, raw = v.split(":", 1)
            return (t.strip(), raw.strip())

        node_type, raw = parse_node_id(node_id)
        if not raw:
            raise ApiError("BAD_REQUEST", "invalid node_id", 400)

        with SessionLocal() as db:
            data: Dict[str, Any] = {}
            via = "mysql"
            if neo4j_driver:
                via = "neo4j"
                try:
                    data = _neo4j_explore(neo4j_driver, node_type=node_type, raw=raw, depth=depth, expand=expand)
                except Exception:
                    via = "mysql"
                    data = _mysql_explore(db, node_type=node_type, raw=raw, expand=expand)
            else:
                data = _mysql_explore(db, node_type=node_type, raw=raw, expand=expand)

            resource_ids = data.get("resource_ids") or []
            current = get_current_user(db)
            if current:
                roles = {r.name for r in current.roles}
                if "teacher" in roles and "dean" not in roles and "admin" not in roles and resource_ids:
                    allowed_ids = (
                        db.execute(
                            select(Resource.id)
                            .where(Resource.status == "approved")
                            .where(Resource.created_by == current.id)
                            .where(Resource.id.in_(resource_ids))
                        )
                        .scalars()
                        .all()
                    )
                    allowed_set = set(int(x) for x in allowed_ids)
                    resource_ids = [rid for rid in resource_ids if int(rid) in allowed_set]
                    data["resource_ids"] = resource_ids
                    nodes = data.get("nodes") or []
                    keep_node_ids = set()
                    for n in nodes:
                        nid = str(n.get("id") or "")
                        if nid.startswith("res:"):
                            try:
                                rid = int(nid.split(":", 1)[1])
                            except Exception:
                                continue
                            if rid in allowed_set:
                                keep_node_ids.add(nid)
                        else:
                            keep_node_ids.add(nid)
                    nodes = [n for n in nodes if str(n.get("id") or "") in keep_node_ids]
                    links = data.get("links") or []
                    links = [l for l in links if str(l.get("source") or "") in keep_node_ids and str(l.get("target") or "") in keep_node_ids]
                    data["nodes"] = nodes
                    data["links"] = links
            items: List[Dict[str, Any]] = []
            if resource_ids:
                resources = (
                    db.execute(select(Resource).where(Resource.status == "approved").where(Resource.id.in_(resource_ids)))
                    .scalars()
                    .all()
                )
                by_id = {r.id: r for r in resources}
                ordered = [by_id[rid] for rid in resource_ids if rid in by_id]
                items = [_resource_dto(r) for r in ordered]

            nodes = data.get("nodes") or []
            if items:
                title_by_id = {int(i["id"]): str(i.get("title") or "") for i in items if i.get("id") is not None}
                for n in nodes:
                    if n.get("type") != "resource":
                        continue
                    nid = str(n.get("id") or "")
                    if not nid.startswith("res:"):
                        continue
                    try:
                        rid = int(nid.split(":", 1)[1])
                    except Exception:
                        continue
                    title = title_by_id.get(rid)
                    if title:
                        n["label"] = title

            return jsonify(
                {
                    "ok": True,
                    "via": via,
                    "nodes": nodes,
                    "links": data.get("links") or [],
                    "items": items,
                    "paths": data.get("paths") or {},
                }
            )

    @app.post("/api/graph/import")
    def graph_import():
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"admin", "dean"})

        if not neo4j_driver:
            raise ApiError("SERVICE_UNAVAILABLE", "Neo4j not configured", 503)
        payload = _json()
        stats = _neo4j_import(neo4j_driver, payload)
        return jsonify({"ok": True, "stats": stats})

    @app.post("/api/graph/sync")
    def graph_sync():
        reset_raw = (request.args.get("reset") or "").strip().lower()
        reset = reset_raw in {"1", "true", "yes", "y", "on"}
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"admin", "dean"})
        stats = sync_neo4j(reset=reset)
        return jsonify({"ok": True, "stats": stats})

    def _admin_count(db: Session, exclude_user_id: Optional[int] = None) -> int:
        q = select(func.count(User.id)).join(User.roles).where(Role.name == "admin").where(User.is_active.is_(True))
        if exclude_user_id is not None:
            q = q.where(User.id != exclude_user_id)
        return int(db.execute(q).scalar_one() or 0)

    def _ensure_user_profiles(db: Session, user: User, role_names: Iterable[str]) -> None:
        roles = set(role_names)
        username = str(user.username or "").strip()
        if not username:
            return

        if "student" in roles:
            s = db.execute(select(Student).where(Student.user_id == user.id)).scalar_one_or_none()
            if not s:
                s2 = db.execute(select(Student).where(Student.student_id == username)).scalar_one_or_none()
                if s2 and s2.user_id is None:
                    s2.user_id = user.id
                elif not s2:
                    db.add(Student(user_id=user.id, student_id=username, name=username))

        if "dean" in roles:
            d = db.execute(select(Dean).where(Dean.user_id == user.id)).scalar_one_or_none()
            if not d:
                d2 = db.execute(select(Dean).where(Dean.dean_id == username)).scalar_one_or_none()
                if d2 and d2.user_id is None:
                    d2.user_id = user.id
                elif not d2:
                    db.add(Dean(user_id=user.id, dean_id=username, name=username))

        if "teacher" in roles:
            t = db.execute(select(Teacher).where(Teacher.user_id == user.id)).scalar_one_or_none()
            if not t:
                t2 = db.execute(select(Teacher).where(Teacher.name == username)).scalar_one_or_none()
                if t2 and t2.user_id is None:
                    t2.user_id = user.id
                elif not t2:
                    db.add(Teacher(user_id=user.id, name=username))

    def _user_dto(u: User) -> Dict[str, Any]:
        return {
            "id": u.id,
            "username": u.username,
            "roles": [r.name for r in u.roles],
            "phone": u.phone,
        }

    def _user_admin_dto(u: User) -> Dict[str, Any]:
        return {
            "id": u.id,
            "username": u.username,
            "roles": [r.name for r in u.roles],
            "is_active": bool(u.is_active),
            "created_at": u.created_at.isoformat() if u.created_at else None,
        }

    @app.get("/api/admin/users")
    def admin_list_users():
        q = (request.args.get("q") or "").strip()
        role = (request.args.get("role") or "").strip().lower()
        if role and role not in {"admin", "dean", "teacher", "student"}:
            raise ApiError("BAD_REQUEST", "invalid role filter", 400)
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"admin", "dean"})
            is_dean = "dean" in {r.name for r in user.roles} and "admin" not in {r.name for r in user.roles}
            stmt = select(User).order_by(User.created_at.desc(), User.id.desc())
            if role:
                if is_dean and role in {"admin", "dean"}:
                    raise ApiError("FORBIDDEN", "Insufficient role", 403)
                stmt = stmt.join(User.roles).where(Role.name == role).distinct()
            if is_dean:
                stmt = stmt.where(~User.roles.any(Role.name.in_(["admin", "dean"])))
            if q:
                stmt = stmt.where(User.username.like(f"%{q}%"))
            users = db.execute(stmt).scalars().all()
            return jsonify({"items": [_user_admin_dto(u) for u in users]})

    @app.post("/api/admin/users")
    def admin_create_user():
        data = _json()
        username = str(data.get("username", "")).strip()
        password = str(data.get("password", "")).strip()
        roles_raw = data.get("roles")
        is_active = data.get("is_active")
        if not username or not password:
            raise ApiError("BAD_REQUEST", "username/password required", 400)

        roles_in: List[str] = []
        if isinstance(roles_raw, list):
            roles_in = [str(r).strip() for r in roles_raw if str(r).strip()]
        if not roles_in:
            roles_in = ["student"]

        with SessionLocal() as db:
            admin = require_auth(db)
            require_roles(admin, {"admin", "dean"})
            admin_roles = {r.name for r in admin.roles}
            is_dean = ("dean" in admin_roles) and ("admin" not in admin_roles)
            if is_dean:
                if set(roles_in).difference({"teacher", "student"}):
                    raise ApiError("FORBIDDEN", "Insufficient role", 403)

            role_rows = db.execute(select(Role).where(Role.name.in_(roles_in))).scalars().all()
            role_by_name = {r.name: r for r in role_rows}
            roles: List[Role] = []
            for name in roles_in:
                if name not in role_by_name:
                    raise ApiError("BAD_REQUEST", f"invalid role: {name}", 400)
                roles.append(role_by_name[name])

            new_user = User(
                username=username,
                password_hash=generate_password_hash(password),
                is_active=bool(is_active) if isinstance(is_active, bool) else True,
            )
            db.add(new_user)
            try:
                db.flush()
            except IntegrityError:
                db.rollback()
                raise ApiError("CONFLICT", "username already exists", 409)

            for r in roles:
                if r not in new_user.roles:
                    new_user.roles.append(r)
            _ensure_user_profiles(db, new_user, [rr.name for rr in new_user.roles])
            db.commit()
            db.refresh(new_user)
            return jsonify({"user": _user_admin_dto(new_user)})

    @app.patch("/api/admin/users/<int:user_id>")
    def admin_update_user(user_id: int):
        data = _json()
        roles_raw = data.get("roles")
        is_active = data.get("is_active")
        password = data.get("password")

        roles_in: Optional[List[str]] = None
        if roles_raw is not None:
            if not isinstance(roles_raw, list):
                raise ApiError("BAD_REQUEST", "roles must be a list", 400)
            roles_in = [str(r).strip() for r in roles_raw if str(r).strip()]
            if not roles_in:
                raise ApiError("BAD_REQUEST", "roles cannot be empty", 400)

        if is_active is not None and not isinstance(is_active, bool):
            raise ApiError("BAD_REQUEST", "is_active must be boolean", 400)
        if password is not None and not isinstance(password, str):
            raise ApiError("BAD_REQUEST", "password must be string", 400)
        if isinstance(password, str) and password.strip() == "":
            password = None

        with SessionLocal() as db:
            admin = require_auth(db)
            require_roles(admin, {"admin", "dean"})
            admin_roles = {r.name for r in admin.roles}
            is_dean = ("dean" in admin_roles) and ("admin" not in admin_roles)

            u = db.get(User, user_id)
            if not u:
                raise ApiError("NOT_FOUND", "user not found", 404)
            target_roles = {r.name for r in u.roles}
            if is_dean:
                if target_roles.intersection({"admin", "dean"}):
                    raise ApiError("FORBIDDEN", "Insufficient role", 403)
                if roles_in is not None and set(roles_in).difference({"teacher", "student"}):
                    raise ApiError("FORBIDDEN", "Insufficient role", 403)

            if u.id == admin.id:
                if is_active is False:
                    raise ApiError("FORBIDDEN", "cannot disable self", 403)
                if roles_in is not None and "admin" not in set(roles_in):
                    raise ApiError("FORBIDDEN", "cannot remove admin role from self", 403)

            current_roles = {r.name for r in u.roles}
            if roles_in is not None:
                next_roles = set(roles_in)
                removing_admin = ("admin" in current_roles) and ("admin" not in next_roles)
                if removing_admin and _admin_count(db, exclude_user_id=u.id) <= 0:
                    raise ApiError("FORBIDDEN", "cannot remove last admin", 403)

                role_rows = db.execute(select(Role).where(Role.name.in_(roles_in))).scalars().all()
                role_by_name = {r.name: r for r in role_rows}
                if len(role_by_name) != len(set(roles_in)):
                    missing = sorted(set(roles_in).difference(set(role_by_name.keys())))
                    raise ApiError("BAD_REQUEST", f"invalid role(s): {', '.join(missing)}", 400)
                u.roles = [role_by_name[name] for name in roles_in]
                _ensure_user_profiles(db, u, [rr.name for rr in u.roles])

            if is_active is not None:
                final_roles = {r.name for r in u.roles}
                if is_active is False and ("admin" in final_roles):
                    if _admin_count(db, exclude_user_id=u.id) <= 0:
                        raise ApiError("FORBIDDEN", "cannot disable last admin", 403)
                u.is_active = is_active

            if password is not None:
                u.password_hash = generate_password_hash(str(password))

            db.commit()
            db.refresh(u)
            return jsonify({"user": _user_admin_dto(u)})

    @app.delete("/api/admin/users/<int:user_id>")
    def admin_delete_user(user_id: int):
        with SessionLocal() as db:
            admin = require_auth(db)
            require_roles(admin, {"admin", "dean"})
            admin_roles = {r.name for r in admin.roles}
            is_dean = ("dean" in admin_roles) and ("admin" not in admin_roles)

            if user_id == admin.id:
                raise ApiError("FORBIDDEN", "cannot delete self", 403)

            u = db.get(User, user_id)
            if not u:
                raise ApiError("NOT_FOUND", "user not found", 404)
            if is_dean:
                target_roles = {r.name for r in u.roles}
                if target_roles.intersection({"admin", "dean"}):
                    raise ApiError("FORBIDDEN", "Insufficient role", 403)

            role_names = {r.name for r in u.roles}
            if "admin" in role_names and u.is_active:
                if _admin_count(db, exclude_user_id=u.id) <= 0:
                    raise ApiError("FORBIDDEN", "cannot delete last admin", 403)

            db.delete(u)
            db.commit()
            return jsonify({"ok": True})

    @app.get("/api/admin/rbac")
    def admin_get_rbac():
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"admin"})

            roles = db.execute(select(Role).order_by(Role.id.asc())).scalars().all()
            permissions = db.execute(select(Permission).order_by(Permission.code.asc())).scalars().all()

            role_items = []
            for r in roles:
                role_items.append(
                    {
                        "id": r.id,
                        "name": r.name,
                        "permissions": [p.code for p in (r.permissions or [])],
                    }
                )
            perm_items = [{"id": p.id, "code": p.code} for p in permissions]
            return jsonify({"roles": role_items, "permissions": perm_items})

    @app.post("/api/admin/permissions")
    def admin_create_permission():
        data = _json()
        code = str(data.get("code", "")).strip()
        if not code:
            raise ApiError("BAD_REQUEST", "code required", 400)

        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"admin"})

            existing = db.execute(select(Permission).where(Permission.code == code)).scalar_one_or_none()
            if existing:
                return jsonify({"permission": {"id": existing.id, "code": existing.code}})

            p = Permission(code=code)
            db.add(p)
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
                existing2 = db.execute(select(Permission).where(Permission.code == code)).scalar_one_or_none()
                if existing2:
                    return jsonify({"permission": {"id": existing2.id, "code": existing2.code}})
                raise ApiError("CONFLICT", "permission already exists", 409)
            db.refresh(p)
            return jsonify({"permission": {"id": p.id, "code": p.code}})

    @app.put("/api/admin/roles/<int:role_id>/permissions")
    def admin_set_role_permissions(role_id: int):
        data = _json()
        codes_raw = data.get("permission_codes")
        if not isinstance(codes_raw, list):
            raise ApiError("BAD_REQUEST", "permission_codes must be a list", 400)
        codes = [str(c).strip() for c in codes_raw if str(c).strip()]
        codes = list(dict.fromkeys(codes))

        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"admin"})

            role = db.get(Role, role_id)
            if not role:
                raise ApiError("NOT_FOUND", "role not found", 404)

            if not codes:
                role.permissions = []
                db.commit()
                db.refresh(role)
                return jsonify({"role": {"id": role.id, "name": role.name, "permissions": []}})

            existing = db.execute(select(Permission).where(Permission.code.in_(codes))).scalars().all()
            by_code = {p.code: p for p in existing}
            missing = [c for c in codes if c not in by_code]
            if missing:
                for c in missing:
                    db.add(Permission(code=c))
                try:
                    db.flush()
                except IntegrityError:
                    db.rollback()
                    pass
                existing = db.execute(select(Permission).where(Permission.code.in_(codes))).scalars().all()
                by_code = {p.code: p for p in existing}

            role.permissions = [by_code[c] for c in codes if c in by_code]
            db.commit()
            db.refresh(role)
            return jsonify({"role": {"id": role.id, "name": role.name, "permissions": [p.code for p in role.permissions]}})

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
    app.seed_demo = seed_demo  # type: ignore[attr-defined]
    app.neo4j_driver = neo4j_driver  # type: ignore[attr-defined]
    app.sync_neo4j = sync_neo4j  # type: ignore[attr-defined]
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
    for role_name in ["student", "teacher", "dean", "admin"]:
        if role_name not in existing_roles:
            db.add(Role(name=role_name))
    db.commit()

    default_permissions = [
        "admin.users.manage",
        "admin.rbac.manage",
        "admin.audit.manage",
        "admin.logs.view",
        "graph.sync",
        "resource.upload",
        "resource.audit",
        "resource.view",
        "resource.favorite",
    ]
    existing_perms = {p.code for p in db.execute(select(Permission)).scalars().all()}
    for code in default_permissions:
        if code not in existing_perms:
            db.add(Permission(code=code))
    db.commit()

    role_by_name = {r.name: r for r in db.execute(select(Role)).scalars().all()}
    perm_by_code = {p.code: p for p in db.execute(select(Permission)).scalars().all()}

    admin_role = role_by_name.get("admin")
    if admin_role:
        want = [perm_by_code[c] for c in default_permissions if c in perm_by_code]
        admin_role.permissions = want
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

def _split_names(raw: str) -> List[str]:
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
        out.append(t[:120])
    return out[:50]


def _user_dto(u: User) -> Dict[str, Any]:
    return {"id": u.id, "username": u.username, "roles": [r.name for r in u.roles], "phone": u.phone}


def _resource_dto(r: Resource) -> Dict[str, Any]:
    course_label = r.course.name if r.course else r.course_name
    kp_label = r.knowledge_point.name if r.knowledge_point else r.knowledge_point_name
    teachers = []
    for rt in getattr(r, "resource_teachers", []) or []:
        if rt.teacher:
            teachers.append({"id": rt.teacher.id, "name": rt.teacher.name})
    return {
        "id": r.id,
        "title": r.title,
        "description": r.description,
        "file_name": r.file_name,
        "file_size": r.file_size,
        "file_type": r.file_type,
        "course_id": r.course_id,
        "knowledge_point_id": r.knowledge_point_id,
        "course": course_label,
        "knowledge_point": kp_label,
        "teachers": teachers,
        "status": r.status,
        "audit_comment": r.audit_comment,
        "audited_by": r.audited_by,
        "audited_at": r.audited_at.isoformat() if r.audited_at else None,
        "tags": [t.tag for t in r.tags],
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "updated_at": r.updated_at.isoformat() if r.updated_at else None,
    }


def _neo4j_overview(driver, course: Optional[str], level: str) -> Dict[str, Any]:
    if level == "courses":
        nodes: Dict[str, Dict[str, Any]] = {}

        def upsert_node(node_id: str, label: str, ntype: str) -> None:
            nodes.setdefault(node_id, {"id": node_id, "label": label, "type": ntype})

        with driver.session() as session:
            records = (
                session.run(
                    """
                    MATCH (c:Course)
                    WHERE $course IS NULL OR c.name = $course
                    RETURN DISTINCT c.name as course
                    """,
                    {"course": course},
                ).data()
                or []
            )
        for rec in records:
            c = rec.get("course")
            if not c:
                continue
            upsert_node(f"course:{c}", c, "course")
        return {"nodes": list(nodes.values()), "links": [], "source": "neo4j"}

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


def _neo4j_search_resource_paths(driver, knowledge: str, limit: int = 200) -> Dict[int, List[str]]:
    query_v4plus = """
    MATCH (k:KnowledgePoint {name: $knowledge})
    CALL {
      WITH k
      MATCH p=(k)-[:RELATED_RESOURCE]->(r:Resource)
      RETURN r.id as rid, nodes(p) as ns, relationships(p) as rs
      UNION
      WITH k
      MATCH p=(k)<-[:HAS_KP]-(c:Course)-[:HAS_RESOURCE]->(r:Resource)
      RETURN r.id as rid, nodes(p) as ns, relationships(p) as rs
      UNION
      WITH k
      MATCH p=(k)-[:RELATED|:PREREQUISITE*1..2]-(k2:KnowledgePoint)-[:RELATED_RESOURCE]->(r:Resource)
      RETURN r.id as rid, nodes(p) as ns, relationships(p) as rs
    }
    RETURN rid,
           [n IN ns | {labels: labels(n), name: coalesce(n.name, n.title, toString(n.id)), id: coalesce(n.id, n.name)}] as nodes,
           [rel IN rs | type(rel)] as rels
    LIMIT $limit
    """

    query_legacy = """
    MATCH (k:KnowledgePoint {name: $knowledge})
    MATCH p=(k)-[:RELATED_RESOURCE]->(r:Resource)
    RETURN r.id as rid,
           [n IN nodes(p) | {labels: labels(n), name: coalesce(n.name, n.title, toString(n.id)), id: coalesce(n.id, n.name)}] as nodes,
           [rel IN relationships(p) | type(rel)] as rels
    LIMIT 100
    UNION
    MATCH (k:KnowledgePoint {name: $knowledge})
    MATCH p=(k)<-[:HAS_KP]-(c:Course)-[:HAS_RESOURCE]->(r:Resource)
    RETURN r.id as rid,
           [n IN nodes(p) | {labels: labels(n), name: coalesce(n.name, n.title, toString(n.id)), id: coalesce(n.id, n.name)}] as nodes,
           [rel IN relationships(p) | type(rel)] as rels
    LIMIT 100
    UNION
    MATCH (k:KnowledgePoint {name: $knowledge})
    MATCH p=(k)-[:RELATED|:PREREQUISITE*1..2]-(k2:KnowledgePoint)-[:RELATED_RESOURCE]->(r:Resource)
    RETURN r.id as rid,
           [n IN nodes(p) | {labels: labels(n), name: coalesce(n.name, n.title, toString(n.id)), id: coalesce(n.id, n.name)}] as nodes,
           [rel IN relationships(p) | type(rel)] as rels
    LIMIT 100
    """

    def node_label(n: Dict[str, Any]) -> str:
        labels = n.get("labels") or []
        label = labels[0] if labels else "Node"
        name = str(n.get("name") or "").strip()
        return f"{label}:{name}" if name else label

    out: Dict[int, List[str]] = {}
    rows: List[Dict[str, Any]] = []
    with driver.session() as session:
        try:
            rows = session.run(query_v4plus, {"knowledge": knowledge, "limit": int(limit)}).data()
        except Exception:
            try:
                rows = session.run(query_legacy, {"knowledge": knowledge}).data()
            except Exception:
                return {}
    for row in rows:
        rid = row.get("rid")
        if rid is None:
            continue
        try:
            rid_int = int(rid)
        except Exception:
            continue
        nodes = row.get("nodes") or []
        rels = row.get("rels") or []
        if not nodes:
            continue
        parts: List[str] = [node_label(nodes[0])]
        for i, rel in enumerate(rels):
            parts.append(f"-{rel}->")
            if i + 1 < len(nodes):
                parts.append(node_label(nodes[i + 1]))
        path_text = " ".join(parts).strip()
        if not path_text:
            continue
        bucket = out.setdefault(rid_int, [])
        if path_text not in bucket:
            bucket.append(path_text)
    for rid, paths in out.items():
        out[rid] = paths[:5]
    return out


def _neo4j_explore(driver, node_type: str, raw: str, depth: int, expand: str) -> Dict[str, Any]:
    depth = depth if depth in {1, 2} else 2
    if expand not in {"full", "kps", "resources"}:
        expand = "full"

    nodes: Dict[str, Dict[str, Any]] = {}
    links: List[Dict[str, Any]] = []
    seen_links = set()
    resource_ids: List[int] = []
    paths: Dict[int, List[str]] = {}

    def upsert_node(node_id: str, label: str, ntype: str) -> None:
        nodes.setdefault(node_id, {"id": node_id, "label": label, "type": ntype})

    def add_link(source: str, target: str, rel_type: str) -> None:
        key = (source, target, rel_type)
        if key in seen_links:
            return
        seen_links.add(key)
        links.append({"source": source, "target": target, "type": rel_type})

    def add_resource_id(rid: Any) -> None:
        if rid is None:
            return
        try:
            rid_int = int(rid)
        except Exception:
            return
        if rid_int in resource_ids:
            return
        resource_ids.append(rid_int)
        upsert_node(f"res:{rid_int}", str(rid_int), "resource")

    if node_type == "kp":
        center_id = f"kp:{raw}"
        upsert_node(center_id, raw, "knowledge_point")
        if expand == "full":
            paths = _neo4j_search_resource_paths(driver, raw)
        with driver.session() as session:
            for row in (
                session.run(
                    """
                    MATCH (c:Course)-[:HAS_KP]->(k:KnowledgePoint {name: $name})
                    RETURN DISTINCT c.name as course
                    """,
                    {"name": raw},
                ).data()
                or []
            ):
                c = row.get("course")
                if not c:
                    continue
                cid = f"course:{c}"
                upsert_node(cid, c, "course")
                add_link(cid, center_id, "has_kp")

            if expand == "full":
                neighbor_rows = (
                    session.run(
                        """
                        MATCH (k:KnowledgePoint {name:$name})<-[:PREREQUISITE]-(p:KnowledgePoint)
                        RETURN p.name as other, 'PREREQUISITE' as rel, 'in' as dir
                        UNION
                        MATCH (k:KnowledgePoint {name:$name})-[:PREREQUISITE]->(n:KnowledgePoint)
                        RETURN n.name as other, 'PREREQUISITE' as rel, 'out' as dir
                        UNION
                        MATCH (k:KnowledgePoint {name:$name})-[r:RELATED]-(x:KnowledgePoint)
                        RETURN x.name as other, 'RELATED' as rel, CASE WHEN startNode(r)=k THEN 'out' ELSE 'in' END as dir
                        """,
                        {"name": raw},
                    ).data()
                    or []
                )
                for row in neighbor_rows:
                    other = row.get("other")
                    if not other or other == raw:
                        continue
                    other_id = f"kp:{other}"
                    upsert_node(other_id, other, "knowledge_point")
                    rel = str(row.get("rel") or "").upper()
                    direction = str(row.get("dir") or "").lower()
                    if rel == "PREREQUISITE":
                        if direction == "in":
                            add_link(other_id, center_id, "prerequisite")
                        else:
                            add_link(center_id, other_id, "prerequisite")
                    elif rel == "RELATED":
                        if direction == "in":
                            add_link(other_id, center_id, "related")
                        else:
                            add_link(center_id, other_id, "related")

            depth_max = 1 if depth == 1 else 2
            resource_query = """
            MATCH (k:KnowledgePoint {name: $name})-[:RELATED_RESOURCE]->(r:Resource)
            RETURN 'kp' as src_type, k.name as src, r.id as rid
            """
            if expand == "full":
                resource_query = f"""
                MATCH (k:KnowledgePoint {{name: $name}})-[:RELATED_RESOURCE]->(r:Resource)
                RETURN 'kp' as src_type, k.name as src, r.id as rid
                UNION
                MATCH (k:KnowledgePoint {{name: $name}})-[:RELATED|:PREREQUISITE*1..{depth_max}]-(k2:KnowledgePoint)-[:RELATED_RESOURCE]->(r:Resource)
                RETURN 'kp' as src_type, k2.name as src, r.id as rid
                UNION
                MATCH (k:KnowledgePoint {{name: $name}})<-[:HAS_KP]-(c:Course)-[:HAS_RESOURCE]->(r:Resource)
                RETURN 'course' as src_type, c.name as src, r.id as rid
                """
            for row in (session.run(resource_query, {"name": raw}).data() or []):
                rid = row.get("rid")
                add_resource_id(rid)
                try:
                    rid_int = int(rid)
                except Exception:
                    continue
                src_type = str(row.get("src_type") or "")
                src = row.get("src")
                if not src:
                    continue
                if src_type == "course":
                    sid = f"course:{src}"
                    upsert_node(sid, src, "course")
                    add_link(sid, f"res:{rid_int}", "has_resource")
                else:
                    sid = f"kp:{src}"
                    upsert_node(sid, src, "knowledge_point")
                    add_link(sid, f"res:{rid_int}", "related_resource")

            if resource_ids:
                teacher_rows = (
                    session.run(
                        """
                        MATCH (r:Resource)
                        WHERE r.id IN $ids
                        OPTIONAL MATCH (t:Teacher)-[:AUTHORED]->(r)
                        RETURN r.id as rid, collect(distinct t.name) as teachers
                        """,
                        {"ids": resource_ids},
                    ).data()
                    or []
                )
                for row in teacher_rows:
                    rid = row.get("rid")
                    if rid is None:
                        continue
                    try:
                        rid_int = int(rid)
                    except Exception:
                        continue
                    for tname in row.get("teachers") or []:
                        if not tname:
                            continue
                        tid = f"teacher:{tname}"
                        upsert_node(tid, tname, "teacher")
                        add_link(tid, f"res:{rid_int}", "authored")

    elif node_type == "course":
        center_id = f"course:{raw}"
        upsert_node(center_id, raw, "course")
        with driver.session() as session:
            kp_rows = (
                session.run(
                    """
                    MATCH (c:Course {name: $name})-[:HAS_KP]->(k:KnowledgePoint)
                    RETURN DISTINCT k.name as kp
                    """,
                    {"name": raw},
                ).data()
                or []
            )
            for row in kp_rows:
                kp = row.get("kp")
                if not kp:
                    continue
                kid = f"kp:{kp}"
                upsert_node(kid, kp, "knowledge_point")
                add_link(center_id, kid, "has_kp")

            if expand == "kps":
                return {"nodes": list(nodes.values()), "links": links, "resource_ids": [], "paths": {}}

            for row in (
                session.run(
                    """
                    MATCH (c:Course {name: $name})-[:HAS_RESOURCE]->(r:Resource)
                    RETURN r.id as rid, 'course' as src_type, c.name as src
                    UNION
                    MATCH (c:Course {name: $name})-[:HAS_KP]->(k:KnowledgePoint)-[:RELATED_RESOURCE]->(r:Resource)
                    RETURN r.id as rid, 'kp' as src_type, k.name as src
                    """,
                    {"name": raw},
                ).data()
                or []
            ):
                rid = row.get("rid")
                add_resource_id(rid)
                try:
                    rid_int = int(rid)
                except Exception:
                    continue
                src_type = str(row.get("src_type") or "")
                src = row.get("src")
                if not src:
                    continue
                if src_type == "course":
                    add_link(center_id, f"res:{rid_int}", "has_resource")
                else:
                    sid = f"kp:{src}"
                    upsert_node(sid, src, "knowledge_point")
                    add_link(sid, f"res:{rid_int}", "related_resource")

            if resource_ids:
                teacher_rows = (
                    session.run(
                        """
                        MATCH (r:Resource)
                        WHERE r.id IN $ids
                        OPTIONAL MATCH (t:Teacher)-[:AUTHORED]->(r)
                        RETURN r.id as rid, collect(distinct t.name) as teachers
                        """,
                        {"ids": resource_ids},
                    ).data()
                    or []
                )
                for row in teacher_rows:
                    rid = row.get("rid")
                    if rid is None:
                        continue
                    try:
                        rid_int = int(rid)
                    except Exception:
                        continue
                    for tname in row.get("teachers") or []:
                        if not tname:
                            continue
                        tid = f"teacher:{tname}"
                        upsert_node(tid, tname, "teacher")
                        add_link(tid, f"res:{rid_int}", "authored")

    elif node_type == "teacher":
        center_id = f"teacher:{raw}"
        upsert_node(center_id, raw, "teacher")
        with driver.session() as session:
            res_rows = (
                session.run(
                    """
                    MATCH (t:Teacher {name: $name})-[:AUTHORED]->(r:Resource)
                    RETURN DISTINCT r.id as rid
                    """,
                    {"name": raw},
                ).data()
                or []
            )
            for row in res_rows:
                rid = row.get("rid")
                add_resource_id(rid)
                try:
                    rid_int = int(rid)
                except Exception:
                    continue
                add_link(center_id, f"res:{rid_int}", "authored")

            for row in (
                session.run(
                    """
                    MATCH (t:Teacher {name: $name})-[:AUTHORED]->(r:Resource)
                    OPTIONAL MATCH (k:KnowledgePoint)-[:RELATED_RESOURCE]->(r)
                    OPTIONAL MATCH (c:Course)-[:HAS_RESOURCE]->(r)
                    RETURN DISTINCT r.id as rid, k.name as kp, c.name as course
                    """,
                    {"name": raw},
                ).data()
                or []
            ):
                rid = row.get("rid")
                if rid is None:
                    continue
                try:
                    rid_int = int(rid)
                except Exception:
                    continue
                kp = row.get("kp")
                if kp:
                    kid = f"kp:{kp}"
                    upsert_node(kid, kp, "knowledge_point")
                    add_link(kid, f"res:{rid_int}", "related_resource")
                course = row.get("course")
                if course:
                    cid = f"course:{course}"
                    upsert_node(cid, course, "course")
                    add_link(cid, f"res:{rid_int}", "has_resource")

    elif node_type == "res":
        try:
            rid_int = int(raw)
        except Exception:
            return {"nodes": [], "links": [], "resource_ids": [], "paths": {}}
        center_id = f"res:{rid_int}"
        upsert_node(center_id, str(rid_int), "resource")
        resource_ids.append(rid_int)
        with driver.session() as session:
            for row in (
                session.run(
                    """
                    MATCH (k:KnowledgePoint)-[:RELATED_RESOURCE]->(r:Resource {id: $id})
                    RETURN DISTINCT k.name as kp
                    """,
                    {"id": rid_int},
                ).data()
                or []
            ):
                kp = row.get("kp")
                if not kp:
                    continue
                kid = f"kp:{kp}"
                upsert_node(kid, kp, "knowledge_point")
                add_link(kid, center_id, "related_resource")

            for row in (
                session.run(
                    """
                    MATCH (c:Course)-[:HAS_RESOURCE]->(r:Resource {id: $id})
                    RETURN DISTINCT c.name as course
                    """,
                    {"id": rid_int},
                ).data()
                or []
            ):
                c = row.get("course")
                if not c:
                    continue
                cid = f"course:{c}"
                upsert_node(cid, c, "course")
                add_link(cid, center_id, "has_resource")

            for row in (
                session.run(
                    """
                    MATCH (t:Teacher)-[:AUTHORED]->(r:Resource {id: $id})
                    RETURN DISTINCT t.name as teacher
                    """,
                    {"id": rid_int},
                ).data()
                or []
            ):
                tname = row.get("teacher")
                if not tname:
                    continue
                tid = f"teacher:{tname}"
                upsert_node(tid, tname, "teacher")
                add_link(tid, center_id, "authored")

    return {"nodes": list(nodes.values()), "links": links, "resource_ids": resource_ids, "paths": paths}


def _mysql_explore(db: Session, node_type: str, raw: str, expand: str) -> Dict[str, Any]:
    if expand not in {"full", "kps", "resources"}:
        expand = "full"

    nodes: Dict[str, Dict[str, Any]] = {}
    links: List[Dict[str, Any]] = []
    seen_links = set()
    resource_ids: List[int] = []

    def upsert_node(node_id: str, label: str, ntype: str) -> None:
        nodes.setdefault(node_id, {"id": node_id, "label": label, "type": ntype})

    def add_link(source: str, target: str, rel_type: str) -> None:
        key = (source, target, rel_type)
        if key in seen_links:
            return
        seen_links.add(key)
        links.append({"source": source, "target": target, "type": rel_type})

    def add_resource(r: Resource, via_kp: Optional[str] = None, via_course: Optional[str] = None) -> None:
        if r.id not in resource_ids:
            resource_ids.append(r.id)
        upsert_node(f"res:{r.id}", r.title or str(r.id), "resource")
        kp_label = via_kp
        if kp_label is None:
            kp_label = r.knowledge_point.name if r.knowledge_point else r.knowledge_point_name
        if kp_label:
            kid = f"kp:{kp_label}"
            upsert_node(kid, kp_label, "knowledge_point")
            add_link(kid, f"res:{r.id}", "related_resource")
        course_label = via_course
        if course_label is None:
            course_label = r.course.name if r.course else r.course_name
        if course_label:
            cid = f"course:{course_label}"
            upsert_node(cid, course_label, "course")
            add_link(cid, f"res:{r.id}", "has_resource")
        for rt in getattr(r, "resource_teachers", []) or []:
            if not rt.teacher:
                continue
            tname = rt.teacher.name
            if not tname:
                continue
            tid = f"teacher:{tname}"
            upsert_node(tid, tname, "teacher")
            add_link(tid, f"res:{r.id}", "authored")

    if node_type == "kp":
        center_id = f"kp:{raw}"
        upsert_node(center_id, raw, "knowledge_point")
        kp = db.execute(select(KnowledgePoint).where(KnowledgePoint.name == raw)).scalar_one_or_none()
        if kp and kp.course:
            cid = f"course:{kp.course.name}"
            upsert_node(cid, kp.course.name, "course")
            add_link(cid, center_id, "has_kp")
        q = select(Resource).where(Resource.status == "approved").order_by(Resource.created_at.desc())
        if kp:
            q = q.where((Resource.knowledge_point_id == kp.id) | (Resource.knowledge_point_name == raw))
        else:
            q = q.where(Resource.knowledge_point_name == raw)
        for r in db.execute(q).scalars().all():
            add_resource(r, via_kp=raw)

    elif node_type == "course":
        center_id = f"course:{raw}"
        upsert_node(center_id, raw, "course")
        c = db.execute(select(Course).where(Course.name == raw)).scalar_one_or_none()
        if c:
            kps = (
                db.execute(select(KnowledgePoint).where(KnowledgePoint.course_id == c.id).order_by(KnowledgePoint.id.asc()))
                .scalars()
                .all()
            )
            for k in kps:
                if not k.name:
                    continue
                kid = f"kp:{k.name}"
                upsert_node(kid, k.name, "knowledge_point")
                add_link(center_id, kid, "has_kp")
        if expand == "kps":
            return {"nodes": list(nodes.values()), "links": links, "resource_ids": [], "paths": {}}
        q = select(Resource).where(Resource.status == "approved").order_by(Resource.created_at.desc())
        if c:
            q = q.where((Resource.course_id == c.id) | (Resource.course_name == raw))
        else:
            q = q.where(Resource.course_name == raw)
        for r in db.execute(q).scalars().all():
            add_resource(r, via_course=raw)

    elif node_type == "teacher":
        center_id = f"teacher:{raw}"
        upsert_node(center_id, raw, "teacher")
        t = db.execute(select(Teacher).where(Teacher.name == raw)).scalar_one_or_none()
        if not t:
            return {"nodes": list(nodes.values()), "links": links, "resource_ids": [], "paths": {}}
        ids = (
            db.execute(select(ResourceTeacher.resource_id).where(ResourceTeacher.teacher_id == t.id))
            .scalars()
            .all()
        )
        if not ids:
            return {"nodes": list(nodes.values()), "links": links, "resource_ids": [], "paths": {}}
        resources = (
            db.execute(select(Resource).where(Resource.status == "approved").where(Resource.id.in_(ids)).order_by(Resource.created_at.desc()))
            .scalars()
            .all()
        )
        for r in resources:
            add_resource(r)
            add_link(center_id, f"res:{r.id}", "authored")

    elif node_type == "res":
        try:
            rid = int(raw)
        except Exception:
            return {"nodes": [], "links": [], "resource_ids": [], "paths": {}}
        r = db.get(Resource, rid)
        if not r or r.status != "approved":
            upsert_node(f"res:{rid}", str(rid), "resource")
            return {"nodes": list(nodes.values()), "links": links, "resource_ids": [rid], "paths": {}}
        upsert_node(f"res:{r.id}", r.title or str(r.id), "resource")
        resource_ids.append(r.id)
        add_resource(r)

    return {"nodes": list(nodes.values()), "links": links, "resource_ids": resource_ids, "paths": {}}


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

    kp_names = (
        db.execute(
            select(KnowledgePoint.name)
            .join(Resource, Resource.knowledge_point_id == KnowledgePoint.id)
            .where(Resource.id.in_(favored))
        )
        .scalars()
        .all()
    )
    kp_set = {kp for kp in kp_names if kp}

    candidates = db.execute(select(Resource).where(Resource.status == "approved")).scalars().all()
    out: List[Tuple[int, str]] = []

    for r in candidates:
        if r.id in favored_set:
            continue
        score = 0
        reasons: List[str] = []
        kp_name = r.knowledge_point.name if r.knowledge_point else r.knowledge_point_name
        if kp_name and kp_name in kp_set:
            score += 10
            reasons.append(f"因为你学习/收藏了知识点 {kp_name}")
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
        kp_name = r.knowledge_point.name if r.knowledge_point else r.knowledge_point_name
        if kp_name and kp_name in kp_set:
            score += 10
            reasons.append(f"因为你学习了知识点 {kp_name}，为你推荐关联资源 {r.title}")
        if score == 0 and neo4j_driver and kp_name:
            related_ids = set(_neo4j_search_resources(neo4j_driver, kp_name))
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
    parser.add_argument("command", nargs="?", default="run", choices=["run", "init-db", "seed-demo", "sync-neo4j", "env"])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=5000, type=int)
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()

    if args.command == "env":
        _print_env_hint()
        raise SystemExit(0)

    if args.command == "init-db":
        app.init_db()
        print("OK")
        raise SystemExit(0)

    if args.command == "seed-demo":
        result = app.seed_demo()  # type: ignore[attr-defined]
        print(json.dumps(result, ensure_ascii=False, indent=2))
        raise SystemExit(0)

    if args.command == "sync-neo4j":
        result = app.sync_neo4j(reset=bool(args.reset))  # type: ignore[attr-defined]
        print(json.dumps(result, ensure_ascii=False, indent=2))
        raise SystemExit(0)

    app.init_db()
    app.run(host=args.host, port=args.port, debug=True)
