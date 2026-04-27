import datetime as dt
import json
import os
import re
import uuid
import traceback
import requests
import jieba
import jieba.analyse
import jieba.posseg as pseg
from difflib import SequenceMatcher
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
    update,
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
    # LLM Configuration (e.g., Qwen, OpenAI)
    llm_api_key: Optional[str]
    llm_base_url: str
    llm_model: str

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
            llm_api_key=os.getenv("LLM_API_KEY"),
            llm_base_url=os.getenv("LLM_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
            llm_model=os.getenv("LLM_MODEL", "qwen-plus"),
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
    department: Mapped[Optional[str]] = mapped_column(String(120), nullable=True) # deprecated
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())

    course_majors: Mapped[List["CourseMajor"]] = relationship("CourseMajor", back_populates="course", cascade="all, delete-orphan")
    chapters: Mapped[List["Chapter"]] = relationship("Chapter", back_populates="course", cascade="all, delete-orphan")


class Major(Base):
    __tablename__ = "majors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    department_id: Mapped[Optional[int]] = mapped_column(ForeignKey("departments.id"), nullable=True)

    department: Mapped[Optional["Department"]] = relationship("Department", back_populates="majors")


class Department(Base):
    __tablename__ = "departments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    majors: Mapped[List[Major]] = relationship("Major", back_populates="department", cascade="all, delete-orphan")


class CourseMajor(Base):
    __tablename__ = "course_majors"
    __table_args__ = (UniqueConstraint("course_id", "major_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    course_id: Mapped[int] = mapped_column(ForeignKey("courses.id"), nullable=False)
    major_id: Mapped[int] = mapped_column(ForeignKey("majors.id"), nullable=False)

    course: Mapped[Course] = relationship("Course", back_populates="course_majors")
    major: Mapped[Major] = relationship("Major")


class Chapter(Base):
    __tablename__ = "chapters"
    __table_args__ = (UniqueConstraint("course_id", "name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    course_id: Mapped[int] = mapped_column(ForeignKey("courses.id"), nullable=False)
    order_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())

    course: Mapped[Course] = relationship("Course", back_populates="chapters")
    sections: Mapped[List["Section"]] = relationship("Section", back_populates="chapter", cascade="all, delete-orphan")


class Section(Base):
    __tablename__ = "sections"
    __table_args__ = (UniqueConstraint("chapter_id", "name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    chapter_id: Mapped[int] = mapped_column(ForeignKey("chapters.id"), nullable=False)
    order_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())

    chapter: Mapped[Chapter] = relationship("Chapter", back_populates="sections")
    knowledge_points: Mapped[List["KnowledgePoint"]] = relationship("KnowledgePoint", back_populates="section")


class KnowledgePoint(Base):
    __tablename__ = "knowledge_points"
    __table_args__ = (UniqueConstraint("name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    course_id: Mapped[Optional[int]] = mapped_column(ForeignKey("courses.id"), nullable=True)
    chapter_id: Mapped[Optional[int]] = mapped_column(ForeignKey("chapters.id"), nullable=True)
    section_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sections.id"), nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())

    course: Mapped[Optional[Course]] = relationship("Course")
    chapter: Mapped[Optional[Chapter]] = relationship("Chapter")
    section: Mapped[Optional[Section]] = relationship("Section", back_populates="knowledge_points")


class Blacklist(Base):
    __tablename__ = "blacklist"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    word: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    course_id: Mapped[Optional[int]] = mapped_column(ForeignKey("courses.id"), nullable=True)  # NULL表示全局黑名单
    reason: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())

    course: Mapped[Optional[Course]] = relationship("Course")


class Lexicon(Base):
    """
    词库模型：存储白名单、学科专业术语及缩写映射。
    word: 原始词（如 "IR"）
    mapping: 归一化后的标准词（如 "指令寄存器"），如果为 NULL 则表示该词本身就是标准词
    """
    __tablename__ = "lexicon"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    word: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    mapping: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
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
    class_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())


class Dean(Base):
    __tablename__ = "deans"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True)
    dean_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    email: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())


class CourseTeacher(Base):
    __tablename__ = "course_teachers"
    __table_args__ = (UniqueConstraint("course_id", "teacher_id", "class_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    course_id: Mapped[int] = mapped_column(ForeignKey("courses.id"), nullable=False)
    teacher_id: Mapped[int] = mapped_column(ForeignKey("teachers.id"), nullable=False)
    class_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

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
    chapter_id: Mapped[Optional[int]] = mapped_column(ForeignKey("chapters.id"), nullable=True)
    section_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sections.id"), nullable=True)
    knowledge_point_id: Mapped[Optional[int]] = mapped_column(ForeignKey("knowledge_points.id"), nullable=True)
    course_name: Mapped[Optional[str]] = mapped_column("course", String(120), nullable=True)
    chapter_name: Mapped[Optional[str]] = mapped_column("chapter", String(200), nullable=True)
    section_name: Mapped[Optional[str]] = mapped_column("section", String(200), nullable=True)
    knowledge_point_name: Mapped[Optional[str]] = mapped_column("knowledge_point", String(120), nullable=True)
    created_by: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    status: Mapped[str] = mapped_column(
        Enum("pending", "approved", "rejected", name="resource_status"),
        nullable=False,
        default="pending",
    )
    audit_comment: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    suggestion: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    audited_by: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True)
    audited_at: Mapped[Optional[dt.datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    course: Mapped[Optional[Course]] = relationship("Course", foreign_keys=[course_id])
    chapter: Mapped[Optional[Chapter]] = relationship("Chapter", foreign_keys=[chapter_id])
    section: Mapped[Optional[Section]] = relationship("Section", foreign_keys=[section_id])
    knowledge_point: Mapped[Optional[KnowledgePoint]] = relationship("KnowledgePoint", foreign_keys=[knowledge_point_id])
    tags: Mapped[List["ResourceTag"]] = relationship("ResourceTag", back_populates="resource", cascade="all, delete-orphan")
    resource_teachers: Mapped[List["ResourceTeacher"]] = relationship("ResourceTeacher", back_populates="resource", cascade="all, delete-orphan")
    resource_knowledge_points: Mapped[List["ResourceKnowledgePoint"]] = relationship("ResourceKnowledgePoint", back_populates="resource", cascade="all, delete-orphan")


class ResourceTag(Base):
    __tablename__ = "resource_tags"
    __table_args__ = (UniqueConstraint("resource_id", "tag"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    resource_id: Mapped[int] = mapped_column(ForeignKey("resources.id"), nullable=False)
    tag: Mapped[str] = mapped_column(String(64), nullable=False)

    resource: Mapped[Resource] = relationship("Resource", back_populates="tags")


class ResourceKnowledgePoint(Base):
    __tablename__ = "resource_knowledge_points"
    __table_args__ = (UniqueConstraint("resource_id", "knowledge_point_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    resource_id: Mapped[int] = mapped_column(ForeignKey("resources.id"), nullable=False)
    knowledge_point_id: Mapped[int] = mapped_column(ForeignKey("knowledge_points.id"), nullable=False)

    resource: Mapped["Resource"] = relationship("Resource", back_populates="resource_knowledge_points")
    knowledge_point: Mapped["KnowledgePoint"] = relationship("KnowledgePoint")


class ResourceTeacher(Base):
    __tablename__ = "resource_teachers"
    __table_args__ = (UniqueConstraint("resource_id", "teacher_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    resource_id: Mapped[int] = mapped_column(ForeignKey("resources.id"), nullable=False)
    teacher_id: Mapped[int] = mapped_column(ForeignKey("teachers.id"), nullable=False)

    resource: Mapped[Resource] = relationship("Resource", back_populates="resource_teachers")
    teacher: Mapped[Teacher] = relationship("Teacher")


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


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    content: Mapped[Text] = mapped_column(Text, nullable=False)
    is_read: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    related_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())

    user: Mapped[User] = relationship("User")


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
            session.run("CREATE CONSTRAINT major_name IF NOT EXISTS FOR (m:Major) REQUIRE m.name IS UNIQUE")
            session.run("CREATE CONSTRAINT department_name IF NOT EXISTS FOR (d:Department) REQUIRE d.name IS UNIQUE")
            session.run(
                "CREATE CONSTRAINT knowledge_point_name IF NOT EXISTS FOR (k:KnowledgePoint) REQUIRE k.name IS UNIQUE"
            )

            session.run("CREATE CONSTRAINT teacher_name IF NOT EXISTS FOR (t:Teacher) REQUIRE t.name IS UNIQUE")
            session.run("CREATE CONSTRAINT resource_id IF NOT EXISTS FOR (r:Resource) REQUIRE r.id IS UNIQUE")
            session.run("CREATE CONSTRAINT chapter_id IF NOT EXISTS FOR (c:Chapter) REQUIRE c.id IS UNIQUE")
            session.run("CREATE CONSTRAINT section_id IF NOT EXISTS FOR (s:Section) REQUIRE s.id IS UNIQUE")

    def _neo4j_upsert_chapter(chapter: Chapter, course_name: Optional[str] = None) -> None:
        if not neo4j_driver:
            return
        try:
            _neo4j_ensure_constraints()
            with neo4j_driver.session() as session:
                session.run(
                    """
                    MERGE (ch:Chapter {id: $id})
                    SET ch.name = $name, ch.order_index = $order_index
                    """,
                    {"id": chapter.id, "name": chapter.name, "order_index": chapter.order_index},
                )
                cname = course_name or (chapter.course.name if chapter.course else None)
                if cname:
                    session.run(
                        """
                        MERGE (c:Course {name: $course_name})
                        MERGE (ch:Chapter {id: $id})
                        MERGE (c)-[:HAS_CHAPTER]->(ch)
                        """,
                        {"course_name": cname, "id": chapter.id},
                    )
        except Exception:
            return

    def _neo4j_delete_chapter(chapter_id: int) -> None:
        if not neo4j_driver:
            return
        try:
            with neo4j_driver.session() as session:
                session.run("MATCH (ch:Chapter {id: $id}) DETACH DELETE ch", {"id": chapter_id})
        except Exception:
            return

    def _neo4j_upsert_section(section: Section, chapter_id: Optional[int] = None) -> None:
        if not neo4j_driver:
            return
        try:
            _neo4j_ensure_constraints()
            with neo4j_driver.session() as session:
                session.run(
                    """
                    MERGE (s:Section {id: $id})
                    SET s.name = $name, s.order_index = $order_index
                    """,
                    {"id": section.id, "name": section.name, "order_index": section.order_index},
                )
                chid = chapter_id or section.chapter_id
                if chid:
                    session.run(
                        """
                        MERGE (ch:Chapter {id: $chapter_id})
                        MERGE (s:Section {id: $id})
                        MERGE (ch)-[:HAS_SECTION]->(s)
                        """,
                        {"chapter_id": chid, "id": section.id},
                    )
        except Exception:
            return

    def _neo4j_delete_section(section_id: int) -> None:
        if not neo4j_driver:
            return
        try:
            with neo4j_driver.session() as session:
                session.run("MATCH (s:Section {id: $id}) DETACH DELETE s", {"id": section_id})
        except Exception:
            return

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
                
                # Use CourseMajor relation
                majors_info = []
                for cm in course.course_majors:
                    if cm.major:
                        m_info = {"name": cm.major.name}
                        if cm.major.department:
                            m_info["dept"] = cm.major.department.name
                        majors_info.append(m_info)
                
                # Detach existing major links
                session.run("MATCH (c:Course {name: $name})-[r:BELONGS_TO_MAJOR]->(:Major) DELETE r", {"name": course.name})
                
                for m in majors_info:
                    session.run(
                        """
                        MERGE (major:Major {name: $major_name})
                        MERGE (c:Course {name: $course_name})
                        MERGE (c)-[:BELONGS_TO_MAJOR]->(major)
                        """,
                        {"major_name": m["name"], "course_name": course.name},
                    )
                    if m.get("dept"):
                        session.run(
                            """
                            MERGE (dept:Department {name: $dept_name})
                            MERGE (major:Major {name: $major_name})
                            MERGE (major)-[:BELONGS_TO_DEPT]->(dept)
                            """,
                            {"dept_name": m["dept"], "major_name": m["name"]},
                        )

        except Exception:
            return

    def _neo4j_upsert_course_teacher(course_id: int, teacher_id: int) -> None:
        if not neo4j_driver:
            return
        try:
            _neo4j_ensure_constraints()
            with SessionLocal() as db:
                course = db.get(Course, course_id)
                teacher = db.get(Teacher, teacher_id)
                if not course or not teacher:
                    return
                with neo4j_driver.session() as session:
                    session.run(
                        """
                        MERGE (t:Teacher {name: $tname})
                        MERGE (c:Course {name: $cname})
                        MERGE (t)-[:TEACHES]->(c)
                        """,
                        {"tname": teacher.name, "cname": course.name},
                    )
        except Exception:
            return

    def _neo4j_upsert_chapter(ch: Chapter) -> None:
        if not neo4j_driver:
            return
        try:
            _neo4j_ensure_constraints()
            with neo4j_driver.session() as session:
                session.run(
                    """
                    MERGE (ch:Chapter {id: $id})
                    SET ch.name = $name
                    """,
                    {"id": ch.id, "name": ch.name},
                )
                if ch.course:
                    session.run(
                        """
                        MERGE (c:Course {name: $cname})
                        MERGE (ch:Chapter {id: $id})
                        MERGE (c)-[:HAS_CHAPTER]->(ch)
                        """,
                        {"cname": ch.course.name, "id": ch.id},
                    )
        except Exception:
            pass

    def _neo4j_upsert_section(s: Section) -> None:
        if not neo4j_driver:
            return
        try:
            _neo4j_ensure_constraints()
            with neo4j_driver.session() as session:
                session.run(
                    """
                    MERGE (s:Section {id: $id})
                    SET s.name = $name
                    """,
                    {"id": s.id, "name": s.name},
                )
                if s.chapter:
                    session.run(
                        """
                        MERGE (ch:Chapter {id: $chid})
                        MERGE (s:Section {id: $id})
                        MERGE (ch)-[:HAS_SECTION]->(s)
                        """,
                        {"chid": s.chapter_id, "id": s.id},
                    )
        except Exception:
            pass

    def _neo4j_upsert_kp(kp: KnowledgePoint, course: Optional[Course]) -> None:
        if not neo4j_driver:
            return
        try:
            _neo4j_ensure_constraints()
            with neo4j_driver.session() as session:
                session.run("MERGE (k:KnowledgePoint {name: $name})", {"name": kp.name})
                
                # 清理逻辑：如果当前知识点有更深层的归属（小节或章节），则必须删除它与课程的直接 HAS_KP 关系
                if kp.section_id or kp.chapter_id:
                    session.run(
                        "MATCH (c:Course)-[r:HAS_KP]->(k:KnowledgePoint {name: $name}) DELETE r",
                        {"name": kp.name}
                    )

                # 建立层级关联，遵循 优先小节 > 其次章节 > 最后课程 的原则
                if kp.section_id:
                    session.run(
                        """
                        MERGE (s:Section {id: $section_id})
                        MERGE (k:KnowledgePoint {name: $kp_name})
                        MERGE (s)-[:HAS_KP]->(k)
                        """,
                        {"section_id": kp.section_id, "kp_name": kp.name},
                    )
                elif kp.chapter_id:
                    session.run(
                        """
                        MERGE (ch:Chapter {id: $chapter_id})
                        MERGE (k:KnowledgePoint {name: $kp_name})
                        MERGE (ch)-[:HAS_KP]->(k)
                        """,
                        {"chapter_id": kp.chapter_id, "kp_name": kp.name},
                    )
                elif course and course.name:
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

    def _neo4j_delete_kp(name: str) -> None:
        if not neo4j_driver:
            return
        try:
            with neo4j_driver.session() as session:
                session.run("MATCH (k:KnowledgePoint {name: $name}) DETACH DELETE k", {"name": name})
        except Exception:
            return

    def _neo4j_upsert_resource(res: Resource) -> None:
        if not neo4j_driver:
            return
        try:
            _neo4j_ensure_constraints()
            with neo4j_driver.session() as session:
                # Upsert Resource node
                session.run(
                    """
                    MERGE (r:Resource {id: $id})
                    SET r.title = $title, r.status = $status, r.file_type = $file_type
                    """,
                    {"id": res.id, "title": res.title, "status": res.status, "file_type": res.file_type},
                )

                # 清理逻辑：如果资源有更深层归属，删除它与课程的直接 HAS_RESOURCE 关系
                if res.section_id or res.chapter_id:
                    session.run(
                        "MATCH (c:Course)-[r:HAS_RESOURCE]->(res:Resource {id: $id}) DELETE r",
                        {"id": res.id}
                    )

                # Link to Level (Section > Chapter > Course)
                if res.section_id:
                    session.run(
                        """
                        MERGE (s:Section {id: $section_id})
                        MERGE (r:Resource {id: $id})
                        MERGE (s)-[:HAS_RESOURCE]->(r)
                        """,
                        {"section_id": res.section_id, "id": res.id},
                    )
                elif res.chapter_id:
                    session.run(
                        """
                        MERGE (ch:Chapter {id: $chapter_id})
                        MERGE (r:Resource {id: $id})
                        MERGE (ch)-[:HAS_RESOURCE]->(r)
                        """,
                        {"chapter_id": res.chapter_id, "id": res.id},
                    )
                else:
                    course_name = res.course.name if res.course else res.course_name
                    if course_name:
                        session.run(
                            """
                            MERGE (c:Course {name: $course_name})
                            MERGE (r:Resource {id: $id})
                            MERGE (c)-[:HAS_RESOURCE]->(r)
                            """,
                            {"course_name": course_name, "id": res.id},
                        )

                # Link to KnowledgePoints (supports multiple)
                if res.resource_knowledge_points:
                    for rkp in res.resource_knowledge_points:
                        kp = rkp.knowledge_point
                        if not kp:
                            continue
                        
                        # 1. 建立知识点与资源的关联
                        session.run(
                            """
                            MERGE (k:KnowledgePoint {name: $kp_name})
                            MERGE (r:Resource {id: $id})
                            MERGE (k)-[:RELATED_RESOURCE]->(r)
                            """,
                            {"kp_name": kp.name, "id": res.id},
                        )

                        # 2. 跨章节关联逻辑 (Cross-chapter Association)
                        # 如果知识点的归属（Section/Chapter）与资源的归属不同，则建立关联
                        # 场景：知识点在第三章，但资源上传到了第五章
                        if res.section_id:
                            # 如果资源在小节下，且知识点不在该小节
                            if kp.section_id != res.section_id:
                                session.run(
                                    """
                                    MERGE (k:KnowledgePoint {name: $kp_name})
                                    MERGE (s:Section {id: $section_id})
                                    MERGE (k)-[:ASSOCIATED_WITH]->(s)
                                    """,
                                    {"kp_name": kp.name, "section_id": res.section_id}
                                )
                        elif res.chapter_id:
                            # 如果资源在章节下，且知识点不在该章节
                            if kp.chapter_id != res.chapter_id:
                                session.run(
                                    """
                                    MERGE (k:KnowledgePoint {name: $kp_name})
                                    MERGE (ch:Chapter {id: $chapter_id})
                                    MERGE (k)-[:ASSOCIATED_WITH]->(ch)
                                    """,
                                    {"kp_name": kp.name, "chapter_id": res.chapter_id}
                                )
                
                # Fallback to single fields if no relations found (compatible with legacy data)
                else:
                    kp_name = res.knowledge_point.name if res.knowledge_point else res.knowledge_point_name
                    if kp_name:
                        # Split by comma in case it's a concatenated string
                        kps_to_link = [k.strip() for k in kp_name.split(",") if k.strip()]
                        for kname in kps_to_link:
                            session.run(
                                """
                                MERGE (k:KnowledgePoint {name: $kp_name})
                                MERGE (r:Resource {id: $id})
                                MERGE (k)-[:RELATED_RESOURCE]->(r)
                                """,
                                {"kp_name": kname, "id": res.id},
                            )
        except Exception:
            return


    def _neo4j_delete_resource(resource_id: int, title: Optional[str]) -> None:
        if not neo4j_driver:
            return
        try:
            _neo4j_ensure_constraints()
            with neo4j_driver.session() as session:
                session.run("MATCH (r:Resource {id: $id}) DETACH DELETE r", {"id": int(resource_id)})
                if title:
                    session.run(
                        "MATCH ()-[m:MENTIONS {source_resource: $title}]->() DELETE m",
                        {"title": str(title)},
                    )
        except Exception:
            return

    def _delete_resource_with_relations(db: Session, res: Resource) -> Dict[str, Any]:
        rid = int(res.id)
        title = str(res.title or "")
        file_path = str(res.file_path or "")

        behaviors = db.execute(select(UserBehavior).where(UserBehavior.resource_id == rid)).scalars().all()
        for b in behaviors:
            db.delete(b)

        notifications = db.execute(select(Notification).where(Notification.related_id == rid)).scalars().all()
        for n in notifications:
            db.delete(n)

        tags = db.execute(select(ResourceTag).where(ResourceTag.resource_id == rid)).scalars().all()
        for t in tags:
            db.delete(t)

        db.delete(res)

        if neo4j_driver:
            _neo4j_delete_resource(rid, title=title)

        removed_file = 0
        if file_path:
            try:
                p = Path(file_path)
                if p.exists():
                    p.unlink()
                    removed_file = 1
            except Exception:
                removed_file = 0

        return {
            "resource_id": rid,
            "deleted": {
                "behaviors": len(behaviors),
                "notifications": len(notifications),
                "tags": len(tags),
                "resource": 1,
                "resource_file": removed_file,
            },
        }


    def _analyze_entity_via_llm(word: str, course_name: Optional[str] = None, current_dir: Optional[str] = None, discipline: Optional[str] = None) -> Dict[str, Any]:
        """
        使用大模型深度分析实体：
        1. 是否为专业知识点
        2. 获取官方全称（归一化）
        3. 领域相关度
        """
        default_res = {"is_knowledge_point": False, "full_name": "", "domain_relevance": 0.0}
        if not settings.llm_api_key or not word or len(word) < 2:
            return default_res

        try:
            prompt = f"""你是一个教学资源管理系统的专家助手。
请分析词汇“{word}”是否是一个合理的专业教学知识点。
上下文：
- 课程名称：{course_name or '未知'}
- 专业领域：{discipline or '未知'}
- 当前所属章节/目录：{current_dir or '未知'}

请严格按照以下 JSON 格式返回结果，不要有任何其他解释文字：
{{
  "is_knowledge_point": true/false,
  "full_name": "如果是缩写或简称，请返回官方学术全称；否则返回原词",
  "domain_relevance": 0.0到1.0之间的浮点数
}}
"""
            headers = {
                "Authorization": f"Bearer {settings.llm_api_key}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": settings.llm_model,
                "messages": [
                    {"role": "system", "content": "你是一个专业的教育领域专家，擅长识别学科知识点并进行实体归一化。"},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.1,
                "response_format": {"type": "json_object"}
            }

            resp = requests.post(settings.llm_base_url + "/chat/completions", json=payload, headers=headers, timeout=5)
            if resp.status_code == 200:
                content = resp.json()["choices"][0]["message"]["content"].strip()
                # 处理可能的 markdown 格式
                if content.startswith("```json"):
                    content = content.split("```json")[1].split("```")[0].strip()
                return json.loads(content)
        except Exception as e:
            print(f"LLM analysis error for '{word}': {e}")

        return default_res

    def _refine_entities_via_llm(raw_candidates: list, course_name: Optional[str] = None, discipline: Optional[str] = None) -> list:
        """
        使用大模型进行实体语义精炼，替代硬编码噪声词清洗。

        流程：
        1. POS过滤：使用 jieba.posseg 仅提取名词性片段
        2. LLM语义蒸馏：交给 Qwen-plus 剔除隐含噪声并对齐同义词
        """
        if not raw_candidates:
            return []

        raw_text = "".join(raw_candidates[:10])

        if not settings.llm_api_key:
            return raw_candidates

        try:
            prompt = f"""你是一个专业的教育领域专家，擅长从文件名中提取学科知识点实体。

## 任务
从文本"{raw_text}"中提取1-3个最核心的知识图谱节点名词。

## 要求
1. 必须是知识图谱节点级别的专业名词（如：操作系统、寄存器、指令寄存器）
2. 剔除任何描述性动作词（如：实现、研究、设计、进行、利用、基于）
3. 剔除指示性词汇（如：第五章、第一节、课后、练习题、任务书）
4. 如果存在缩写或简称，请同时输出全称（如：IR→指令寄存器，PC→程序计数器）
5. 只返回JSON数组，不要其他解释文字

## 输出格式
{{"entities": ["实体1", "实体2"]}}
"""
            headers = {
                "Authorization": f"Bearer {settings.llm_api_key}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": settings.llm_model,
                "messages": [
                    {"role": "system", "content": "你是一个专业的教育领域专家，擅长识别学科知识点并进行实体归一化。"},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.1,
                "response_format": {"type": "json_object"}
            }

            resp = requests.post(settings.llm_base_url + "/chat/completions", json=payload, headers=headers, timeout=15)

            if resp.status_code == 200:
                content = resp.json()["choices"][0]["message"]["content"].strip()
                if content.startswith("```json"):
                    content = content.split("```json")[1].split("```")[0].strip()
                result = json.loads(content)
                entities = result.get("entities", [])
                return entities
        except Exception as e:
            print(f"[!] LLM refinement error: {e}")

        return raw_candidates

    def _process_resource_pipeline(res: Resource, db: Session) -> None:
        """
        增强版自动化数据处理流程：
        1. 系统梳理数据来源，对收集的多源异构数据进行清洗、去重、格式标准化
        2. 批量上传时能够实现“实体识别、关系抽取等处理，构建结构化的教学资源知识图谱”
        """
        import zipfile
        import xml.etree.ElementTree as ET
        import pandas as pd
        import pdfplumber
        from io import BytesIO
        import traceback

        print(f"[*] Starting pipeline for resource: {res.file_name} (ID: {res.id})")
        if not res.file_path or not os.path.exists(res.file_path):
            print(f"[!] File path not found: {res.file_path}")
            return

        suffix = Path(res.file_name).suffix.lower()
        raw_text = ""

        # --- 步骤 1: 文本提取 (根据不同格式提取内容) ---
        try:
            if suffix == ".docx":
                # Word 文件处理: 提取文本内容
                with zipfile.ZipFile(res.file_path) as docx:
                    xml_content = docx.read("word/document.xml")
                    tree = ET.fromstring(xml_content)
                    namespaces = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
                    raw_text = " ".join([node.text for node in tree.findall(".//w:t", namespaces) if node.text])
            
            elif suffix == ".pdf":
                # PDF 处理: 使用 pdfplumber 提取文本内容
                with pdfplumber.open(res.file_path) as pdf:
                    pages_text = []
                    for page in pdf.pages:
                        text = page.extract_text()
                        if text:
                            pages_text.append(text)
                    raw_text = "\n".join(pages_text)
            
            elif suffix == ".pptx":
                # PPT 课件处理: 提取每页文字
                with zipfile.ZipFile(res.file_path) as pptx:
                    slide_texts = []
                    slide_files = [f for f in pptx.namelist() if f.startswith("ppt/slides/slide")]
                    for slide_file in sorted(slide_files):
                        xml_content = pptx.read(slide_file)
                        tree = ET.fromstring(xml_content)
                        namespaces = {"a": "http://schemas.openxmlformats.org/drawingml/2006/main"}
                        slide_texts.append(" ".join([node.text for node in tree.findall(".//a:t", namespaces) if node.text]))
                    raw_text = "\n".join(slide_texts)
            
            elif suffix == ".xlsx":
                # Excel 处理: 读取表格内容，去重、格式化字段
                df = pd.read_excel(res.file_path)
                df = df.drop_duplicates()
                raw_text = df.to_string(index=False)
            
            elif suffix == ".txt":
                # TXT 处理: 直接读取，指定 UTF-8 编码
                try:
                    with open(res.file_path, "r", encoding="utf-8") as f:
                        raw_text = f.read()
                except UnicodeDecodeError:
                    with open(res.file_path, "r", encoding="gbk") as f:
                        raw_text = f.read()
        except Exception as e:
            print(f"[!] Text extraction error: {e}")
            raw_text = f"提取内容失败: {str(e)}"

        # --- 步骤 2: 数据清洗与标准化 ---
        def clean_standardize(text: str) -> str:
            # 清洗空行、多余空格
            text = re.sub(r"\n\s*\n", "\n", text)
            # 清洗特殊符号、乱码、水印（模拟常见水印清洗）
            text = re.sub(r"第\s*\d+\s*页|Page\s*\d+|[^\x00-\x7f\u4e00-\u9fa5，。？！、；：“”‘’（）《》]", " ", text, flags=re.I)
            # 统一文本编码已经在读取时处理，最终统一清洗多余空格
            text = re.sub(r"\s+", " ", text).strip()
            return text
        
        cleaned_text = clean_standardize(raw_text)

        # --- 步骤 3: 自动实体识别与知识图谱构建 (NER & RE) ---
        # 1. 如果资源未关联知识点，尝试从文件名或内容中抽取关键词作为知识点
        file_base = Path(res.file_name).stem
        if not res.knowledge_point_id:
            print(f"[*] Identifying entity for: {file_base}")
            # --- 阶段1: 预处理 (Preprocessing) ---
            # 1.1 仅剔除日期、版本、括号内容，保留核心文本以维持分词上下文
            clean_name = re.sub(r"\[.*?\]|（.*?）|\(.*?\)|《|》", "", file_base)
            clean_name = re.sub(r"\d{4}[-/_]\d{2}([-/_]\d{2})?", "", clean_name)
            clean_name = re.sub(r"v\d+\.\d+|最新版|修订版|最终版", "", clean_name, flags=re.I)
            clean_name = clean_name.strip("- _")

            # 1.2 获取黑名单（用于后续过滤，不再预先擦除）
            try:
                all_black = set(db.execute(
                    select(Blacklist.word).where((Blacklist.course_id == res.course_id) | (Blacklist.course_id.is_(None)))
                ).scalars().all())
            except Exception as e:
                print(f"[!] Error fetching blacklist: {e}")
                all_black = set()

            # 1.3 获取词库（白名单和映射）
            try:
                lexicon_entries = db.execute(
                    select(Lexicon).where((Lexicon.course_id == res.course_id) | (Lexicon.course_id.is_(None)))
                ).scalars().all()
                discipline_dict = {e.word: (e.mapping if e.mapping else e.word) for e in lexicon_entries}
            except Exception as e:
                print(f"[!] Error fetching lexicon: {e}")
                discipline_dict = {}

            # --- 阶段2: 分词干预 (Tokenization Tuning) ---
            whitelist = []
            course = res.course
            if course:
                # 显式加载关联数据
                db.refresh(course)
                whitelist.append(course.name)
                for chap in course.chapters:
                    whitelist.append(chap.name)
                    for sec in chap.sections:
                        whitelist.append(sec.name)
            
            # 注入白名单和学科词库到分词器
            for word in whitelist:
                if len(word) > 1:
                    try:
                        jieba.add_word(word)
                    except:
                        pass
            
            for word in discipline_dict:
                if len(word) > 1:
                    try:
                        jieba.add_word(word)
                    except:
                        pass

            # --- 阶段3: 实体提取策略 (Extraction Logic) ---
            candidate_entities = [] # 候选实体列表
            used_positions = [] # 记录已匹配的位置，避免重叠

            # 策略A：本地词库匹配 (Lexicon Match) - 优先级最高
            # 使用正则匹配而非简单子串匹配，支持中英混杂
            sorted_lexicon = sorted([w for w in discipline_dict.keys() if len(w) > 1], key=len, reverse=True)
            for w in sorted_lexicon:
                # 构造正则：处理纯英文词和纯中文词
                if re.match(r"^[A-Za-z0-9]+$", w):
                    pattern = re.compile(r"\b" + w + r"\b", re.IGNORECASE)
                else:
                    pattern = re.compile(re.escape(w))
                match = pattern.search(clean_name)
                if match:
                    start, end = match.start(), match.end()
                    # 检查是否与已匹配位置重叠
                    if not any(start < ep and end > sp for sp, ep in used_positions):
                        used_positions.append((start, end))
                        if w not in candidate_entities:
                            candidate_entities.append(w)

            # 策略B：优先匹配白名单长词 (Whitelist Match)
            sorted_whitelist = sorted([w for w in whitelist if len(w) > 1], key=len, reverse=True)
            for w in sorted_whitelist:
                if w in clean_name and w not in candidate_entities:
                    candidate_entities.append(w)
            
            # 策略C：学术名词与缩写识别 (Regex Match)
            # 优化正则：移除了 \b，改用更灵活的匹配方式，支持中英混排
            # 模式解释：
            # 1. [A-Z0-9]{2,} : 连续大写字母或数字（缩写）
            # 2. (?:指令|通用|专用|状态|变址|基址)?[\u4e00-\u9fa5]*(?:寄存器组?|计数器|单元|电路|控制器|运算器|存储器|总线|接口) : 典型硬件组件名
            academic_patterns = re.findall(r"[A-Z0-9]{2,}|(?:指令|通用|专用|状态|变址|基址)?[\u4e00-\u9fa5]*(?:寄存器组?|计数器|单元|电路|控制器|运算器|存储器|总线|接口)", clean_name)
            for ap in academic_patterns:
                if ap not in candidate_entities and len(ap) >= 2:
                    candidate_entities.append(ap)

            # 策略D：规则启发（词性标注与 TextRank）
            try:
                words = pseg.cut(clean_name)
                valid_tags = {'n', 'nz', 'vn', 'nrt'}
                for w, t in words:
                    if t in valid_tags and len(w) > 1:
                        if w not in candidate_entities:
                            candidate_entities.append(w)
                
                # 增加 TextRank 提取关键词
                keywords = jieba.analyse.textrank(clean_name, topK=5)
                for kw in keywords:
                    if kw not in candidate_entities:
                        candidate_entities.append(kw)
            except Exception as e:
                print(f"[!] Tokenization error: {e}")

            # --- 阶段4: 实体清洗与去噪 (LLM语义精炼) ---
            # 使用大模型替代硬编码噪声词进行语义清洗
            try:
                refined_entities = _refine_entities_via_llm(candidate_entities, course_name=res.course.name if res.course else None)
            except Exception as e:
                print(f"[!] LLM call failed: {e}")
                refined_entities = candidate_entities

            extracted_entities = []
            for entity in refined_entities:
                try:
                    # 基础过滤：长度 >= 2 且不是纯数字
                    if len(entity) >= 2 and not re.match(r"^\d+$", entity) and entity not in all_black:
                        if entity not in extracted_entities:
                            extracted_entities.append(entity)
                except Exception as e:
                    print(f"[!] Entity filtering error for '{entity}': {e}")

            if not extracted_entities:
                for entity in candidate_entities:
                    temp = entity.strip("- _")
                    if temp not in all_black and len(temp) >= 2 and not re.match(r"^\d+$", temp):
                        if temp not in extracted_entities:
                            extracted_entities.append(temp)

            # 排序：根据在原文件名中出现的顺序排序，确保第一个提到的实体作为主实体
            extracted_entities.sort(key=lambda x: clean_name.find(x) if x in clean_name else 999)

            # --- 阶段5: 外部知识库校验 (External Validation) & 图谱关联 ---
            final_kp_ids = []
            final_kp_names = []
            
            # 获取学科领域作为上下文
            discipline_name = ""
            if course:
                try:
                    from sqlalchemy.orm import selectinload
                    stmt = select(Course).where(Course.id == res.course_id).options(
                        selectinload(Course.course_majors).selectinload(CourseMajor.major)
                    )
                    course_with_major = db.execute(stmt).scalar_one_or_none()
                    if course_with_major and course_with_major.course_majors:
                        discipline_name = course_with_major.course_majors[0].major.name
                except Exception: pass

            current_dir_name = ""
            if res.section_id:
                sec = db.get(Section, res.section_id)
                if sec: current_dir_name = sec.name
            elif res.chapter_id:
                chap = db.get(Chapter, res.chapter_id)
                if chap: current_dir_name = chap.name

            for entity in extracted_entities:
                # 0. 实体归一化：优先使用本地词库的映射关系 (解决 IR -> 指令寄存器等简称问题)
                target_kp_name = discipline_dict.get(entity, entity)
                existing_kp = None
                
                # 1. 本地匹配：优先查找已有知识点 (使用归一化后的名称)
                existing_kp = db.execute(
                    select(KnowledgePoint).where(KnowledgePoint.course_id == res.course_id).where(KnowledgePoint.name == target_kp_name)
                ).scalar_one_or_none()

                if existing_kp:
                    # 更新 target_kp_name 为数据库中实际存储的名称
                    target_kp_name = existing_kp.name
                else:
                    # 2. 快速通行证：检查原始实体或归一化词是否在本地学科词库中
                    is_discipline_word = (entity in discipline_dict or target_kp_name in discipline_dict)
                    
                    if is_discipline_word:
                        print(f"[*] Entity '{entity}' (normalized to '{target_kp_name}') matched in local discipline dictionary.")
                        # 本地库命中的直接作为新知识点名称，此时 target_kp_name 已经是全称
                    else:
                        # 3. 外部知识库验证 (LLM 驱动版)
                        if settings.llm_api_key:
                            llm_result = _analyze_entity_via_llm(entity, course.name if course else None, current_dir_name, discipline_name)
                            
                            if not llm_result.get("is_knowledge_point"):
                                # 容错：如果领域相关度很高，即使 is_knowledge_point 为 false 也尝试保留
                                if llm_result.get("domain_relevance", 0) < 0.6:
                                    print(f"[*] Entity '{entity}' rejected by LLM. Adding to blacklist.")
                                    try:
                                        new_black = Blacklist(word=entity, course_id=res.course_id, reason="LLM 自动识别为非知识点")
                                        db.add(new_black)
                                        db.flush()
                                    except Exception: pass
                                    continue
                            
                            # 实体归一化
                            full_name = llm_result.get("full_name")
                            if full_name and len(full_name) >= 2:
                                target_kp_name = full_name
                                # 归一化后再次检查是否已存在
                                existing_kp = db.execute(
                                    select(KnowledgePoint).where(KnowledgePoint.course_id == res.course_id).where(KnowledgePoint.name == target_kp_name)
                                ).scalar_one_or_none()

                # 4. 关联或创建
                try:
                    if not existing_kp:
                        new_kp = KnowledgePoint(
                            name=target_kp_name, 
                            course_id=res.course_id,
                            chapter_id=res.chapter_id,
                            section_id=res.section_id
                        )
                        db.add(new_kp)
                        db.flush()
                        existing_kp = new_kp
                        print(f"[*] New KP created: {target_kp_name}")
                    else:
                        print(f"[*] Reusing existing KP: {target_kp_name} (ID: {existing_kp.id})")
                    
                    if existing_kp.id not in final_kp_ids:
                        final_kp_ids.append(existing_kp.id)
                        final_kp_names.append(existing_kp.name)
                        rkp_exists = any(rkp.knowledge_point_id == existing_kp.id for rkp in res.resource_knowledge_points)
                        if not rkp_exists:
                            res.resource_knowledge_points.append(ResourceKnowledgePoint(knowledge_point_id=existing_kp.id))
                except Exception as e:
                    print(f"[!] Error associating KP '{target_kp_name}': {e}")

            # --- 阶段6: 结果收敛与兜底 ---
            if final_kp_ids:
                res.knowledge_point_id = final_kp_ids[0]
                res.knowledge_point_name = ", ".join(final_kp_names)
            else:
                # 最终兜底：关联到当前目录代表的知识点
                target_kp = None
                if res.section_id:
                    target_kp = db.execute(select(KnowledgePoint).where(KnowledgePoint.section_id == res.section_id)).first()
                if not target_kp and res.chapter_id:
                    target_kp = db.execute(select(KnowledgePoint).where(KnowledgePoint.chapter_id == res.chapter_id, KnowledgePoint.section_id.is_(None))).first()
                
                if target_kp:
                    # 处理 RowProxy 情况
                    kp_obj = target_kp[0] if isinstance(target_kp, tuple) else target_kp
                    res.knowledge_point_id = kp_obj.id
                    res.knowledge_point_name = kp_obj.name
                    exists = any(rkp.knowledge_point_id == kp_obj.id for rkp in res.resource_knowledge_points)
                    if not exists:
                        res.resource_knowledge_points.append(ResourceKnowledgePoint(knowledge_point_id=kp_obj.id))
                elif course:
                    # 如果实在找不到，显示目录名称或课程名称
                    res.knowledge_point_name = current_dir_name or course.name

        # 2. 实体识别 (NER)：识别文中提及的其他已有知识点
        try:
            all_kps = db.execute(
                select(KnowledgePoint).where(KnowledgePoint.course_id == res.course_id)
            ).scalars().all()

            # 3. 关系抽取 (RE)：建立 MENTIONS 关系并同步到 Neo4j
            # 注意：上传阶段不再直接同步到 Neo4j，等待管理员审核通过后同步
            for kp in all_kps:
                # 如果文中提及了其他知识点，建立关联
                if kp.name in cleaned_text and kp.name != res.knowledge_point_name:
                    # if neo4j_driver:
                    #     try:
                    #         with neo4j_driver.session() as session:
                    #             session.run(
                    #                 """
                    #                 MATCH (k1:KnowledgePoint {name: $kp1})
                    #                 MATCH (k2:KnowledgePoint {name: $kp2})
                    #                 MERGE (k1)-[r:MENTIONS]->(k2)
                    #                 SET r.source_resource = $res_title
                    #                 """,
                    #                 {"kp1": res.knowledge_point_name, "kp2": kp.name, "res_title": res.title}
                    #             )
                    #     except Exception:
                    #         pass
                    
                    # 为资源打上对应的知识点标签
                    if kp.name not in [t.tag for t in res.tags]:
                        res.tags.append(ResourceTag(tag=kp.name))
        except Exception as e:
            print(f"[!] NER/RE error: {e}")
            traceback.print_exc()
        
        print(f"[*] Pipeline finished for: {res.file_name}")

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
            _seed_majors(db)
            _backfill_resource_refs(db)
            users = db.execute(select(User)).scalars().all()
            for u in users:
                _ensure_user_profiles(db, u, [r.name for r in (u.roles or [])])
            db.commit()

    def _seed_majors(db: Session) -> None:
        # Seed Departments
        depts_data = ["计算机学院", "数学学院", "外国语学院", "人工智能学院"]
        dept_map = {}
        for d_name in depts_data:
            existing = db.execute(select(Department).where(Department.name == d_name)).scalar_one_or_none()
            if not existing:
                existing = Department(name=d_name)
                db.add(existing)
                db.flush()
            dept_map[d_name] = existing.id

        # Seed Majors with department linkage
        majors_data = [
            ("计算机科学与技术", "计算机学院"),
            ("软件工程", "计算机学院"),
            ("人工智能", "人工智能学院"),
            ("数据科学", "数学学院"),
            ("网络安全", "计算机学院"),
            ("应用数学", "数学学院"),
            ("英语", "外国语学院")
        ]
        for m_name, d_name in majors_data:
            existing = db.execute(select(Major).where(Major.name == m_name)).scalar_one_or_none()
            if not existing:
                db.add(Major(name=m_name, department_id=dept_map.get(d_name)))
            else:
                existing.department_id = dept_map.get(d_name)
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
            if "suggestion" not in cols:
                to_add.append(("suggestion", "JSON"))
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

        if "departments" not in insp.get_table_names():
            Base.metadata.tables["departments"].create(engine_, checkfirst=True)
        if "blacklist" not in insp.get_table_names():
            Base.metadata.tables["blacklist"].create(engine_, checkfirst=True)

        if "resource_teachers" not in insp.get_table_names():
            Base.metadata.tables["resource_teachers"].create(engine_, checkfirst=True)

        if "resource_knowledge_points" not in insp.get_table_names():
            Base.metadata.tables["resource_knowledge_points"].create(engine_, checkfirst=True)
        if "majors" not in insp.get_table_names():
            Base.metadata.tables["majors"].create(engine_, checkfirst=True)
        else:
            cols = {c["name"] for c in insp.get_columns("majors")}
            if "department_id" not in cols:
                with engine_.connect() as conn:
                    conn.exec_driver_sql("ALTER TABLE majors ADD COLUMN department_id INTEGER REFERENCES departments(id)")
                    conn.commit()

        if "course_majors" not in insp.get_table_names():
            Base.metadata.tables["course_majors"].create(engine_, checkfirst=True)
            # Migration: if courses has department, or course_departments exists, try to fill course_majors
            with engine_.connect() as conn:
                # 尝试从旧表迁移数据
                if "course_departments" in insp.get_table_names():
                    old_data = conn.exec_driver_sql("SELECT course_id, department FROM course_departments").fetchall()
                    for cid, dept_name in old_data:
                        # 先确保专业存在
                        conn.exec_driver_sql("INSERT OR IGNORE INTO majors (name) VALUES (%s)", (dept_name,))
                        mid = conn.exec_driver_sql("SELECT id FROM majors WHERE name = %s", (dept_name,)).scalar()
                        if mid:
                            conn.exec_driver_sql("INSERT OR IGNORE INTO course_majors (course_id, major_id) VALUES (%s, %s)", (cid, mid))
                conn.commit()

        if "course_departments" in insp.get_table_names():
            with engine_.connect() as conn:
                conn.exec_driver_sql("DROP TABLE course_departments")
                conn.commit()


        if "notifications" not in insp.get_table_names():
            Base.metadata.tables["notifications"].create(engine_, checkfirst=True)

        if "students" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("students")}
            to_add: List[Tuple[str, str]] = []
            if "user_id" not in cols:
                to_add.append(("user_id", "INTEGER"))
            if "class_name" not in cols:
                to_add.append(("class_name", "VARCHAR(64)"))
            if to_add:
                with engine_.connect() as conn:
                    for name, sql_type in to_add:
                        conn.exec_driver_sql(f"ALTER TABLE students ADD COLUMN {name} {sql_type}")
                    conn.commit()

        if "course_teachers" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("course_teachers")}
            if "class_name" not in cols:
                with engine_.connect() as conn:
                    conn.exec_driver_sql("ALTER TABLE course_teachers ADD COLUMN class_name VARCHAR(64)")
                    conn.commit()

        if "courses" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("courses")}
            if "department" not in cols:
                with engine_.connect() as conn:
                    conn.exec_driver_sql("ALTER TABLE courses ADD COLUMN department VARCHAR(120)")
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

        if "chapters" not in insp.get_table_names():
            Base.metadata.tables["chapters"].create(engine_, checkfirst=True)
        if "sections" not in insp.get_table_names():
            Base.metadata.tables["sections"].create(engine_, checkfirst=True)

        if "knowledge_points" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("knowledge_points")}
            to_add: List[Tuple[str, str]] = []
            if "chapter_id" not in cols:
                to_add.append(("chapter_id", "INTEGER"))
            if "section_id" not in cols:
                to_add.append(("section_id", "INTEGER"))
            if to_add:
                with engine_.connect() as conn:
                    for name, sql_type in to_add:
                        conn.exec_driver_sql(f"ALTER TABLE knowledge_points ADD COLUMN {name} {sql_type}")
                    conn.commit()

        if "resources" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("resources")}
            to_add: List[Tuple[str, str]] = []
            if "chapter_id" not in cols:
                to_add.append(("chapter_id", "INTEGER"))
            if "section_id" not in cols:
                to_add.append(("section_id", "INTEGER"))
            if "chapter" not in cols:
                to_add.append(("chapter", "VARCHAR(200)"))
            if "section" not in cols:
                to_add.append(("section", "VARCHAR(200)"))
            if to_add:
                with engine_.connect() as conn:
                    for name, sql_type in to_add:
                        conn.exec_driver_sql(f"ALTER TABLE resources ADD COLUMN {name} {sql_type}")
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
        token = ""
        if auth.startswith("Bearer "):
            token = auth[len("Bearer ") :].strip()
        else:
            token = request.args.get("token", "")

        if not token:
            return None
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
                    
                    if existing.status == "approved":
                        _neo4j_upsert_resource(existing)
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
                
                if r.status == "approved":
                    _neo4j_upsert_resource(r)
                return r

            admin = ensure_user("admin", "admin123", "admin")
            dean = ensure_user("dean", "dean123", "dean")
            teacher_user = ensure_user("teacher", "teacher123", "teacher")
            student = ensure_user("student", "student123", "student")

            # Link demo teacher user to a profile
            _ensure_user_profiles(db, teacher_user, ["teacher"])
            _ensure_user_profiles(db, student, ["student"])
            _ensure_user_profiles(db, dean, ["dean"])

            c_math = ensure_course("高等数学")
            c_ds = ensure_course("数据结构")

            kp_limit = ensure_kp("极限", c_math)
            kp_derivative = ensure_kp("导数", c_math)
            kp_tree = ensure_kp("二叉树", c_ds)

            # Use the auto-created teacher profile for the 'teacher' user
            t_demo = db.execute(select(Teacher).where(Teacher.user_id == teacher_user.id)).scalar_one()
            t_demo.name = "张老师" # Rename it to match the expected demo name
            db.flush()
            t_zhang = t_demo
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
            chapters = db.execute(select(Chapter)).scalars().all()
            sections = db.execute(select(Section)).scalars().all()
            kps = db.execute(select(KnowledgePoint)).scalars().all()
            teachers = db.execute(select(Teacher)).scalars().all()
            resources = db.execute(select(Resource)).scalars().all()
            links = db.execute(select(ResourceTeacher)).scalars().all()

            course_rows = []
            for c in courses:
                depts = [cd.department for cd in c.course_departments] if c.course_departments else []
                if not depts and c.department:
                    depts = [d.strip() for d in str(c.department).replace("，", ",").split(",") if d.strip()]
                course_rows.append({
                    "name": c.name, 
                    "code": c.code, 
                    "departments": depts, 
                    "description": c.description
                })

            chapter_rows = []
            for ch in chapters:
                cname = ch.course.name if ch.course else None
                chapter_rows.append({
                    "id": ch.id,
                    "name": ch.name,
                    "order_index": ch.order_index,
                    "course_name": cname
                })

            section_rows = []
            for s in sections:
                section_rows.append({
                    "id": s.id,
                    "name": s.name,
                    "order_index": s.order_index,
                    "chapter_id": s.chapter_id
                })

            kp_rows = []
            for k in kps:
                course_name = None
                if k.course_id:
                    c = db.get(Course, k.course_id)
                    course_name = c.name if c else None
                kp_rows.append({
                    "name": k.name, 
                    "course_name": course_name,
                    "chapter_id": k.chapter_id,
                    "section_id": k.section_id
                })

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
                        "chapter_id": r.chapter_id,
                        "section_id": r.section_id
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
            session.run("CREATE CONSTRAINT dept_name IF NOT EXISTS FOR (d:Department) REQUIRE d.name IS UNIQUE")
            session.run(
                "CREATE CONSTRAINT knowledge_point_name IF NOT EXISTS FOR (k:KnowledgePoint) REQUIRE k.name IS UNIQUE"
            )
            session.run("CREATE CONSTRAINT teacher_name IF NOT EXISTS FOR (t:Teacher) REQUIRE t.name IS UNIQUE")
            session.run("CREATE CONSTRAINT resource_id IF NOT EXISTS FOR (r:Resource) REQUIRE r.id IS UNIQUE")
            session.run("CREATE CONSTRAINT chapter_id IF NOT EXISTS FOR (ch:Chapter) REQUIRE ch.id IS UNIQUE")
            session.run("CREATE CONSTRAINT section_id IF NOT EXISTS FOR (s:Section) REQUIRE s.id IS UNIQUE")

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
                    WITH c, row
                    UNWIND (CASE WHEN row.departments IS NOT NULL THEN row.departments ELSE [] END) AS dname
                    MERGE (dept:Department {name: dname})
                    MERGE (c)-[:BELONGS_TO_DEPT]->(dept)
                    """,
                    {"rows": batch},
                )

            for batch in _chunks(chapter_rows, 500):
                session.run(
                    """
                    UNWIND $rows as row
                    MERGE (ch:Chapter {id: row.id})
                    SET ch.name = row.name, ch.order_index = row.order_index
                    WITH ch, row
                    OPTIONAL MATCH (c:Course {name: row.course_name})
                    FOREACH (_ IN CASE WHEN c IS NULL THEN [] ELSE [1] END | MERGE (c)-[:HAS_CHAPTER]->(ch))
                    """,
                    {"rows": batch},
                )

            for batch in _chunks(section_rows, 500):
                session.run(
                    """
                    UNWIND $rows as row
                    MERGE (s:Section {id: row.id})
                    SET s.name = row.name, s.order_index = row.order_index
                    WITH s, row
                    OPTIONAL MATCH (ch:Chapter {id: row.chapter_id})
                    FOREACH (_ IN CASE WHEN ch IS NULL THEN [] ELSE [1] END | MERGE (ch)-[:HAS_SECTION]->(s))
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
                    WITH k, row
                    OPTIONAL MATCH (s:Section {id: row.section_id})
                    FOREACH (_ IN CASE WHEN s IS NULL THEN [] ELSE [1] END | MERGE (s)-[:HAS_KP]->(k))
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
                    WITH r, row
                    OPTIONAL MATCH (s:Section {id: row.section_id})
                    FOREACH (_ IN CASE WHEN s IS NULL THEN [] ELSE [1] END | MERGE (s)-[:HAS_RESOURCE]->(r))
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
            "chapters": len(chapter_rows),
            "sections": len(section_rows),
            "knowledge_points": len(kp_rows),
            "teachers": len(teacher_rows),
            "resources": len(resource_rows),
            "relations": {
                "has_chapter": len([r for r in chapter_rows if r.get("course_name")]),
                "has_section": len([r for r in section_rows if r.get("chapter_id")]),
                "has_kp": len([r for r in kp_rows if r.get("course_name") or r.get("section_id")]),
                "has_resource": len([r for r in resource_rows if r.get("course_name") or r.get("section_id")]),
                "related_resource": len([r for r in resource_rows if r.get("knowledge_point")]),
                "authored": len(authored_rows),
            },
        }
        return stats

    @app.post("/api/admin/graph/sync")
    def sync_graph_full():
        """
        全量同步 Neo4j 数据：
        1. 清理 Neo4j 中的所有节点和关系
        2. 从 MySQL 重新加载所有数据并同步到 Neo4j
        """
        if not neo4j_driver:
            return jsonify({"error": "Neo4j not configured"}), 500
        
        try:
            with neo4j_driver.session() as session:
                # 危险操作：删除 Neo4j 中的所有内容
                session.run("MATCH (n) DETACH DELETE n")
            
            _neo4j_ensure_constraints()
            
            counts = {"departments": 0, "majors": 0, "courses": 0, "chapters": 0, "sections": 0, "knowledge_points": 0, "resources": 0, "assignments": 0}
            
            with SessionLocal() as db:
                # 同步院系
                depts = db.execute(select(Department)).scalars().all()
                with neo4j_driver.session() as session:
                    for d in depts:
                        session.run("MERGE (dept:Department {name: $name})", {"name": d.name})
                        counts["departments"] += 1
                
                # 同步专业
                majors = db.execute(select(Major)).scalars().all()
                with neo4j_driver.session() as session:
                    for m in majors:
                        session.run("MERGE (maj:Major {name: $name})", {"name": m.name})
                        counts["majors"] += 1
                        if m.department:
                            session.run(
                                """
                                MERGE (maj:Major {name: $mname})
                                MERGE (dept:Department {name: $dname})
                                MERGE (maj)-[:BELONGS_TO_DEPT]->(dept)
                                """,
                                {"mname": m.name, "dname": m.department.name}
                            )
                
                # 同步课程
                courses = db.execute(select(Course)).scalars().all()
                for c in courses:
                    _neo4j_upsert_course(c)
                    counts["courses"] += 1
                
                # 同步章节
                chapters = db.execute(select(Chapter)).scalars().all()
                for ch in chapters:
                    _neo4j_upsert_chapter(ch)
                    counts["chapters"] += 1
                
                # 同步小节
                sections = db.execute(select(Section)).scalars().all()
                for s in sections:
                    _neo4j_upsert_section(s)
                    counts["sections"] += 1

                # 同步教学分配 (教师与课程的联系)
                assignments = db.execute(select(CourseTeacher)).scalars().all()
                for a in assignments:
                    _neo4j_upsert_course_teacher(a.course_id, a.teacher_id)
                    counts["assignments"] += 1
                
                # 同步知识点
                kps = db.execute(select(KnowledgePoint)).scalars().all()
                for k in kps:
                    _neo4j_upsert_kp(k, k.course)
                    counts["knowledge_points"] += 1
                
                # 同步资源
                resources = db.execute(select(Resource)).scalars().all()
                for r in resources:
                    _neo4j_upsert_resource(r)
                    counts["resources"] += 1
            
            return jsonify({"ok": True, "message": "Full graph sync completed", "counts": counts})
        except Exception as e:
            import traceback
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500

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
        major_id = _parse_int(request.args.get("major_id"))
        with SessionLocal() as db:
            user = get_current_user(db)
            stmt = select(Course).order_by(Course.created_at.desc())
            
            if q:
                like = f"%{q}%"
                stmt = stmt.where((Course.name.like(like)) | (Course.code.like(like)) | (Course.description.like(like)))
            
            if major_id:
                stmt = stmt.join(CourseMajor).where(CourseMajor.major_id == major_id)

            items = db.execute(stmt).scalars().all()
            return jsonify(
                {
                    "items": [
                        {
                            "id": c.id,
                            "name": c.name,
                            "code": c.code,
                            "majors": [cm.major.name for cm in c.course_majors if cm.major],
                            "description": c.description,
                            "created_at": c.created_at.isoformat(),
                        }
                        for c in items
                    ]
                }
            )

    @app.get("/api/departments")
    def list_departments():
        with SessionLocal() as db:
            items = db.execute(select(Department).order_by(Department.name)).scalars().all()
            return jsonify({"items": [{"id": d.id, "name": d.name} for d in items]})

    @app.get("/api/majors")
    def list_majors():
        dept_id = _parse_int(request.args.get("department_id"))
        with SessionLocal() as db:
            stmt = select(Major).order_by(Major.name)
            if dept_id:
                stmt = stmt.where(Major.department_id == dept_id)
            items = db.execute(stmt).scalars().all()
            return jsonify({"items": [{"id": m.id, "name": m.name, "department_id": m.department_id} for m in items]})


    @app.post("/api/courses")
    def create_course():
        data = _json()
        name = str(data.get("name", "")).strip()
        code = (data.get("code") or "").strip() or None
        major_ids = data.get("major_ids") or []
        description = (data.get("description") or "").strip() or None
        if not name:
            raise ApiError("BAD_REQUEST", "name required", 400)
        
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"dean"})
            existing = db.execute(select(Course).where(Course.name == name)).scalar_one_or_none()
            if existing:
                existing.code = code
                existing.description = description
                
                # Update course_majors
                existing.course_majors.clear()
                for mid in major_ids:
                    existing.course_majors.append(CourseMajor(major_id=mid))
                
                db.commit()
                _neo4j_upsert_course(existing)
                return jsonify({"course": {"id": existing.id, "name": existing.name, "code": existing.code, "description": existing.description}})
            
            c = Course(name=name, code=code, description=description)
            for mid in major_ids:
                c.course_majors.append(CourseMajor(major_id=mid))
            
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

    @app.post("/api/courses/<int:course_id>/cleanup")
    def cleanup_course_resources(course_id: int):
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"dean", "admin"})
            c = db.get(Course, course_id)
            if not c:
                raise ApiError("NOT_FOUND", "course not found", 404)

            resource_rows = db.execute(
                select(Resource).where(
                    (Resource.course_id == c.id) | (Resource.course_name == c.name)
                )
            ).scalars().all()

            deleted_kps = 0
            deleted_chapters = 0
            deleted_sections = 0
            deleted_resources = 0
            removed_files = 0

            for r in resource_rows:
                if r.file_path:
                    try:
                        p = Path(r.file_path)
                        if p.exists():
                            p.unlink()
                            removed_files += 1
                    except Exception:
                        pass
                deleted_resources += 1

            resource_ids = [r.id for r in resource_rows]
            if resource_ids:
                behaviors = db.execute(select(UserBehavior).where(UserBehavior.resource_id.in_(resource_ids))).scalars().all()
                for b in behaviors:
                    db.delete(b)
                tags = db.execute(select(ResourceTag).where(ResourceTag.resource_id.in_(resource_ids))).scalars().all()
                for t in tags:
                    db.delete(t)
                rts = db.execute(select(ResourceTeacher).where(ResourceTeacher.resource_id.in_(resource_ids))).scalars().all()
                for rt in rts:
                    db.delete(rt)
                notifications = db.execute(select(Notification).where(Notification.related_id.in_(resource_ids))).scalars().all()
                for n in notifications:
                    db.delete(n)

            for r in resource_rows:
                db.delete(r)

            sections = db.execute(select(Section).where(Section.chapter_id.in_(
                select(Chapter.id).where(Chapter.course_id == c.id)
            ))).scalars().all()
            for s in sections:
                db.delete(s)
                deleted_sections += 1

            chapters = db.execute(select(Chapter).where(Chapter.course_id == c.id)).scalars().all()
            for ch in chapters:
                db.delete(ch)
                deleted_chapters += 1

            kps = db.execute(select(KnowledgePoint).where(KnowledgePoint.course_id == c.id)).scalars().all()
            for k in kps:
                db.delete(k)
                deleted_kps += 1

            db.commit()

            return jsonify({
                "ok": True,
                "cleaned": {
                    "knowledge_points": deleted_kps,
                    "chapters": deleted_chapters,
                    "sections": deleted_sections,
                    "resources": deleted_resources,
                    "resource_files": removed_files,
                }
            })

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
                    items.append({"id": t.id, "name": t.name, "email": t.email, "class_name": ct.class_name})
            return jsonify({"items": items})

    @app.put("/api/courses/<int:course_id>/teachers")
    def set_course_teachers(course_id: int):
        data = _json()
        assignments = data.get("assignments") or []
        if not isinstance(assignments, list):
            raise ApiError("BAD_REQUEST", "assignments must be a list", 400)
        
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"dean"})
            c = db.get(Course, course_id)
            if not c:
                raise ApiError("NOT_FOUND", "course not found", 404)
            
            # Clear existing assignments
            db.execute(
                select(CourseTeacher).where(CourseTeacher.course_id == course_id)
            )
            existing = db.execute(select(CourseTeacher).where(CourseTeacher.course_id == course_id)).scalars().all()
            for row in existing:
                db.delete(row)
            
            # Add new assignments
            for assign in assignments:
                tid = assign.get("teacher_id")
                cname = assign.get("class_name")
                if not isinstance(tid, int):
                    continue
                t = db.get(Teacher, tid)
                if not t:
                    continue
                db.add(CourseTeacher(course_id=course_id, teacher_id=tid, class_name=cname))
                _neo4j_upsert_course_teacher(course_id, tid)

            
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
            return jsonify({"items": [{
                "id": k.id,
                "name": k.name,
                "course_id": k.course_id,
                "chapter_id": k.chapter_id,
                "section_id": k.section_id
            } for k in items]})

    @app.post("/api/knowledge-points")
    def create_knowledge_point():
        data = _json()
        name = str(data.get("name", "")).strip()
        course_id = data.get("course_id")
        course_name = (data.get("course_name") or "").strip() or None
        chapter_id = _parse_int(data.get("chapter_id"))
        section_id = _parse_int(data.get("section_id"))
        if not name:
            raise ApiError("BAD_REQUEST", "name required", 400)
        with SessionLocal() as db:
            user = require_auth(db)
            roles = {r.name for r in user.roles}
            if "dean" in roles:
                c = _resolve_course(db, int(course_id) if isinstance(course_id, int) else _parse_int(course_id), course_name)
            else:
                require_roles(user, {"teacher"})
                if isinstance(course_id, int):
                    cid = course_id
                else:
                    cid = _parse_int(course_id)
                if cid is None:
                    raise ApiError("FORBIDDEN", "teacher must specify an assigned course", 403)
                c = db.get(Course, cid)
                if not c:
                    raise ApiError("NOT_FOUND", "course not found", 404)
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
            if chapter_id:
                chapter = db.get(Chapter, chapter_id)
                if not chapter:
                    raise ApiError("NOT_FOUND", "chapter not found", 404)
                if chapter.course_id != (c.id if c else None):
                    raise ApiError("FORBIDDEN", "chapter does not belong to this course", 403)
            if section_id:
                section = db.get(Section, section_id)
                if not section:
                    raise ApiError("NOT_FOUND", "section not found", 404)
                if chapter_id and section.chapter_id != chapter_id:
                    raise ApiError("FORBIDDEN", "section does not belong to this chapter", 403)
            existing = db.execute(select(KnowledgePoint).where(KnowledgePoint.name == name)).scalar_one_or_none()
            if existing:
                if c and existing.course_id is None:
                    existing.course_id = c.id
                if chapter_id:
                    existing.chapter_id = chapter_id
                if section_id:
                    existing.section_id = section_id
                db.commit()
                if c:
                    _neo4j_upsert_course(c)
                _neo4j_upsert_kp(existing, c)
                return jsonify({"knowledge_point": {
                    "id": existing.id,
                    "name": existing.name,
                    "course_id": existing.course_id,
                    "chapter_id": existing.chapter_id,
                    "section_id": existing.section_id
                }})
            k = KnowledgePoint(name=name, course_id=c.id if c else None, chapter_id=chapter_id, section_id=section_id)
            db.add(k)
            db.commit()
            db.refresh(k)
            if c:
                _neo4j_upsert_course(c)
            _neo4j_upsert_kp(k, c)
            return jsonify({"knowledge_point": {
                "id": k.id,
                "name": k.name,
                "course_id": k.course_id,
                "chapter_id": k.chapter_id,
                "section_id": k.section_id
            }})

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
            
            kp_name = kp.name
            db.delete(kp)
            db.commit()
            _neo4j_delete_kp(kp_name)
            return jsonify({"ok": True})

    @app.get("/api/chapters")
    def list_chapters():
        course_id = _parse_int(request.args.get("course_id"))
        with SessionLocal() as db:
            q = select(Chapter).order_by(Chapter.order_index, Chapter.created_at)
            if course_id is not None:
                q = q.where(Chapter.course_id == course_id)
            items = db.execute(q).scalars().all()
            return jsonify({
                "items": [{
                    "id": c.id,
                    "name": c.name,
                    "course_id": c.course_id,
                    "order_index": c.order_index,
                    "created_at": c.created_at.isoformat()
                } for c in items]
            })

    @app.post("/api/chapters")
    def create_chapter():
        data = _json()
        name = str(data.get("name", "")).strip()
        course_id = _parse_int(data.get("course_id"))
        order_index = data.get("order_index") or 0
        if not name:
            raise ApiError("BAD_REQUEST", "name required", 400)
        if not course_id:
            raise ApiError("BAD_REQUEST", "course_id required", 400)
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"teacher"})
            c = db.get(Course, course_id)
            if not c:
                raise ApiError("NOT_FOUND", "course not found", 404)
            trow = db.execute(select(Teacher).where(Teacher.user_id == user.id)).scalar_one_or_none()
            if not trow:
                raise ApiError("FORBIDDEN", "teacher profile not found", 403)
            assigned = db.execute(
                select(CourseTeacher).where(CourseTeacher.course_id == course_id).where(CourseTeacher.teacher_id == trow.id)
            ).scalars().first()
            if not assigned:
                raise ApiError("FORBIDDEN", "not assigned to this course", 403)
            chapter = Chapter(name=name, course_id=course_id, order_index=int(order_index))
            db.add(chapter)
            db.commit()
            db.refresh(chapter)
            _neo4j_upsert_chapter(chapter, course_name=c.name)
            return jsonify({"chapter": {
                "id": chapter.id,
                "name": chapter.name,
                "course_id": chapter.course_id,
                "order_index": chapter.order_index
            }})

    @app.patch("/api/chapters/<int:chapter_id>")
    def update_chapter(chapter_id: int):
        data = _json()
        name = data.get("name")
        order_index = data.get("order_index")
        if name is None and order_index is None:
            raise ApiError("BAD_REQUEST", "name or order_index required", 400)
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"teacher"})
            chapter = db.get(Chapter, chapter_id)
            if not chapter:
                raise ApiError("NOT_FOUND", "chapter not found", 404)
            trow = db.execute(select(Teacher).where(Teacher.user_id == user.id)).scalar_one_or_none()
            if not trow:
                raise ApiError("FORBIDDEN", "teacher profile not found", 403)
            assigned = db.execute(
                select(CourseTeacher).where(CourseTeacher.course_id == chapter.course_id).where(CourseTeacher.teacher_id == trow.id)
            ).scalars().first()
            if not assigned:
                raise ApiError("FORBIDDEN", "not assigned to this course", 403)
            if name is not None:
                name = name.strip()
                if not name:
                    raise ApiError("BAD_REQUEST", "name cannot be empty", 400)
                chapter.name = name
            if order_index is not None:
                chapter.order_index = int(order_index)
            db.commit()
            db.refresh(chapter)
            _neo4j_upsert_chapter(chapter) # chapter.course is loaded by relationship
            return jsonify({"chapter": {
                "id": chapter.id,
                "name": chapter.name,
                "course_id": chapter.course_id,
                "order_index": chapter.order_index
            }})

    @app.delete("/api/chapters/<int:chapter_id>")
    def delete_chapter(chapter_id: int):
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"teacher"})
            chapter = db.get(Chapter, chapter_id)
            if not chapter:
                raise ApiError("NOT_FOUND", "chapter not found", 404)
            trow = db.execute(select(Teacher).where(Teacher.user_id == user.id)).scalar_one_or_none()
            if not trow:
                raise ApiError("FORBIDDEN", "teacher profile not found", 403)
            assigned = db.execute(
                select(CourseTeacher).where(CourseTeacher.course_id == chapter.course_id).where(CourseTeacher.teacher_id == trow.id)
            ).scalars().first()
            if not assigned:
                raise ApiError("FORBIDDEN", "not assigned to this course", 403)

            # 1. 删除章节下的所有小节（级联触发小节下的资源和知识点删除）
            sections = db.execute(select(Section).where(Section.chapter_id == chapter_id)).scalars().all()
            for s in sections:
                # 删除该小节下的资源
                s_resources = db.execute(select(Resource).where(Resource.section_id == s.id)).scalars().all()
                for r in s_resources:
                    _delete_resource_with_relations(db, r)
                # 删除该小节下的知识点
                s_kps = db.execute(select(KnowledgePoint).where(KnowledgePoint.section_id == s.id)).scalars().all()
                for kp in s_kps:
                    db.delete(kp)
                    if neo4j_driver:
                        with neo4j_driver.session() as session:
                            session.run("MATCH (k:KnowledgePoint {name: $name}) DETACH DELETE k", {"name": kp.name})
                # 删除小节本身和图谱节点
                db.delete(s)
                _neo4j_delete_section(s.id)

            # 2. 删除直接关联到章节但未关联小节的资源
            c_resources = db.execute(select(Resource).where(Resource.chapter_id == chapter_id).where(Resource.section_id == None)).scalars().all()
            for r in c_resources:
                _delete_resource_with_relations(db, r)

            # 3. 删除直接关联到章节但未关联小节的知识点
            c_kps = db.execute(select(KnowledgePoint).where(KnowledgePoint.chapter_id == chapter_id).where(KnowledgePoint.section_id == None)).scalars().all()
            for kp in c_kps:
                db.delete(kp)
                if neo4j_driver:
                    with neo4j_driver.session() as session:
                        session.run("MATCH (k:KnowledgePoint {name: $name}) DETACH DELETE k", {"name": kp.name})

            # 4. 删除章节本身
            db.delete(chapter)
            db.commit()
            _neo4j_delete_chapter(chapter_id)
            return jsonify({"ok": True})

    @app.get("/api/sections")
    def list_sections():
        chapter_id = _parse_int(request.args.get("chapter_id"))
        course_id = _parse_int(request.args.get("course_id"))
        with SessionLocal() as db:
            q = select(Section).order_by(Section.order_index, Section.created_at)
            if chapter_id is not None:
                q = q.where(Section.chapter_id == chapter_id)
            elif course_id is not None:
                q = q.join(Chapter).where(Chapter.course_id == course_id)
            items = db.execute(q).scalars().all()
            return jsonify({
                "items": [{
                    "id": s.id,
                    "name": s.name,
                    "chapter_id": s.chapter_id,
                    "order_index": s.order_index,
                    "created_at": s.created_at.isoformat()
                } for s in items]
            })

    @app.post("/api/sections")
    def create_section():
        data = _json()
        name = str(data.get("name", "")).strip()
        chapter_id = _parse_int(data.get("chapter_id"))
        order_index = data.get("order_index") or 0
        if not name:
            raise ApiError("BAD_REQUEST", "name required", 400)
        if not chapter_id:
            raise ApiError("BAD_REQUEST", "chapter_id required", 400)
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"teacher"})
            chapter = db.get(Chapter, chapter_id)
            if not chapter:
                raise ApiError("NOT_FOUND", "chapter not found", 404)
            trow = db.execute(select(Teacher).where(Teacher.user_id == user.id)).scalar_one_or_none()
            if not trow:
                raise ApiError("FORBIDDEN", "teacher profile not found", 403)
            assigned = db.execute(
                select(CourseTeacher).where(CourseTeacher.course_id == chapter.course_id).where(CourseTeacher.teacher_id == trow.id)
            ).scalars().first()
            if not assigned:
                raise ApiError("FORBIDDEN", "not assigned to this course", 403)
            section = Section(name=name, chapter_id=chapter_id, order_index=int(order_index))
            db.add(section)
            db.commit()
            db.refresh(section)
            _neo4j_upsert_section(section, chapter_id=chapter_id)
            return jsonify({"section": {
                "id": section.id,
                "name": section.name,
                "chapter_id": section.chapter_id,
                "order_index": section.order_index
            }})

    @app.patch("/api/sections/<int:section_id>")
    def update_section(section_id: int):
        data = _json()
        name = data.get("name")
        order_index = data.get("order_index")
        if name is None and order_index is None:
            raise ApiError("BAD_REQUEST", "name or order_index required", 400)
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"teacher"})
            section = db.get(Section, section_id)
            if not section:
                raise ApiError("NOT_FOUND", "section not found", 404)
            trow = db.execute(select(Teacher).where(Teacher.user_id == user.id)).scalar_one_or_none()
            if not trow:
                raise ApiError("FORBIDDEN", "teacher profile not found", 403)
            chapter = db.get(Chapter, section.chapter_id)
            if not chapter:
                raise ApiError("NOT_FOUND", "chapter not found", 404)
            assigned = db.execute(
                select(CourseTeacher).where(CourseTeacher.course_id == chapter.course_id).where(CourseTeacher.teacher_id == trow.id)
            ).scalars().first()
            if not assigned:
                raise ApiError("FORBIDDEN", "not assigned to this course", 403)
            if name is not None:
                name = name.strip()
                if not name:
                    raise ApiError("BAD_REQUEST", "name cannot be empty", 400)
                section.name = name
            if order_index is not None:
                section.order_index = int(order_index)
            db.commit()
            db.refresh(section)
            _neo4j_upsert_section(section) # section.chapter_id is available
            return jsonify({"section": {
                "id": section.id,
                "name": section.name,
                "chapter_id": section.chapter_id,
                "order_index": section.order_index
            }})

    @app.delete("/api/sections/<int:section_id>")
    def delete_section(section_id: int):
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"teacher"})
            section = db.get(Section, section_id)
            if not section:
                raise ApiError("NOT_FOUND", "section not found", 404)
            trow = db.execute(select(Teacher).where(Teacher.user_id == user.id)).scalar_one_or_none()
            if not trow:
                raise ApiError("FORBIDDEN", "teacher profile not found", 403)
            chapter = db.get(Chapter, section.chapter_id)
            if not chapter:
                raise ApiError("NOT_FOUND", "chapter not found", 404)
            assigned = db.execute(
                select(CourseTeacher).where(CourseTeacher.course_id == chapter.course_id).where(CourseTeacher.teacher_id == trow.id)
            ).scalars().first()
            if not assigned:
                raise ApiError("FORBIDDEN", "not assigned to this course", 403)

            # 1. 删除关联资源
            resources = db.execute(select(Resource).where(Resource.section_id == section_id)).scalars().all()
            for r in resources:
                _delete_resource_with_relations(db, r)

            # 2. 删除关联知识点
            kps = db.execute(select(KnowledgePoint).where(KnowledgePoint.section_id == section_id)).scalars().all()
            for kp in kps:
                # 如果知识点没有关联其他课程/资源，可以完全删除
                # 这里简单处理：直接解除关联或删除
                db.delete(kp)
                # 同时清理图谱中的知识点节点
                if neo4j_driver:
                    with neo4j_driver.session() as session:
                        session.run("MATCH (k:KnowledgePoint {name: $name}) DETACH DELETE k", {"name": kp.name})

            # 3. 删除小节本身
            db.delete(section)
            db.commit()
            _neo4j_delete_section(section_id)
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
                {
                    "items": [
                        {
                            "id": t.id,
                            "name": t.name,
                            "email": t.email,
                            "user_id": t.user_id,
                            "has_user": t.user_id is not None,
                            "created_at": t.created_at.isoformat(),
                        }
                        for t in items
                    ]
                }
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

    @app.get("/api/notifications")
    def list_notifications():
        with SessionLocal() as db:
            user = require_auth(db)
            stmt = (
                select(Notification)
                .where(Notification.user_id == user.id)
                .order_by(Notification.created_at.desc())
            )
            items = db.execute(stmt).scalars().all()
            return jsonify({
                "items": [
                    {
                        "id": n.id,
                        "title": n.title,
                        "content": n.content,
                        "is_read": n.is_read,
                        "type": n.type,
                        "related_id": n.related_id,
                        "created_at": n.created_at.isoformat()
                    }
                    for n in items
                ]
            })

    @app.post("/api/notifications/read-all")
    def read_all_notifications():
        with SessionLocal() as db:
            user = require_auth(db)
            db.execute(
                update(Notification)
                .where(Notification.user_id == user.id)
                .values(is_read=True)
            )
            db.commit()
            return jsonify({"ok": True})

    @app.post("/api/notifications/<int:notification_id>/read")
    def read_notification(notification_id: int):
        with SessionLocal() as db:
            user = require_auth(db)
            n = db.get(Notification, notification_id)
            if not n or n.user_id != user.id:
                raise ApiError("NOT_FOUND", "notification not found", 404)
            n.is_read = True
            db.commit()
            return jsonify({"ok": True})

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
            
            # Find all teacher profiles linked to this user or with the same name if unlinked
            teacher_ids_stmt = select(Teacher.id).where(Teacher.user_id == user.id)
            teacher_ids = set(db.execute(teacher_ids_stmt).scalars().all())
            
            # Fallback: teachers with same name but no user_id
            fallback_stmt = select(Teacher.id).where(Teacher.name == user.username).where(Teacher.user_id.is_(None))
            teacher_ids.update(db.execute(fallback_stmt).scalars().all())

            if not teacher_ids:
                # Still try to ensure a profile exists
                _ensure_user_profiles(db, user, ["teacher"])
                db.commit()
                t = db.execute(select(Teacher).where(Teacher.user_id == user.id)).scalar_one_or_none()
                if t:
                    teacher_ids.add(t.id)
                else:
                    raise ApiError("FORBIDDEN", "teacher profile not found", 403)

            course_ids = (
                db.execute(select(CourseTeacher.course_id).where(CourseTeacher.teacher_id.in_(list(teacher_ids))))
                .scalars()
                .all()
            )
            if not course_ids:
                return jsonify({"items": []})
            
            # Ensure unique course IDs
            course_ids = list(dict.fromkeys(course_ids))
            
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
            chapter_id = _parse_int(request.form.get("chapter_id"))
            section_id = _parse_int(request.form.get("section_id"))
            knowledge_point_id = _parse_int(request.form.get("knowledge_point_id"))
            tags_raw = (request.form.get("tags") or "").strip()

            if not title:
                raise ApiError("BAD_REQUEST", "title is required", 400)
            if course_id is None:
                raise ApiError("BAD_REQUEST", "course_id is required", 400)

            original_name = (f.filename or "upload").strip()
            suffix = Path(original_name).suffix.lower()
            allowed_extensions = {".docx", ".pdf", ".pptx", ".xlsx", ".txt"}
            if suffix not in allowed_extensions:
                raise ApiError("BAD_REQUEST", f"仅支持上传 {', '.join([ext.lstrip('.') for ext in allowed_extensions])} 格式文件", 400)

            file_id = str(uuid.uuid4())
            safe_name = f"{file_id}{suffix}"
            dest_path = settings.upload_dir / safe_name
            f.save(dest_path)

            course_obj = db.get(Course, course_id)
            if not course_obj:
                raise ApiError("NOT_FOUND", "course not found", 404)

            chapter_obj = None
            if chapter_id:
                chapter_obj = db.get(Chapter, chapter_id)
                if not chapter_obj:
                    raise ApiError("NOT_FOUND", "chapter not found", 404)
                if chapter_obj.course_id != course_obj.id:
                    raise ApiError("FORBIDDEN", "chapter does not belong to this course", 403)

            section_obj = None
            if section_id:
                section_obj = db.get(Section, section_id)
                if not section_obj:
                    raise ApiError("NOT_FOUND", "section not found", 404)
                if chapter_id and section_obj.chapter_id != chapter_id:
                    raise ApiError("FORBIDDEN", "section does not belong to this chapter", 403)

            kp_obj = None
            if knowledge_point_id:
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
                chapter_id=chapter_obj.id if chapter_obj else None,
                section_id=section_obj.id if section_obj else None,
                knowledge_point_id=kp_obj.id if kp_obj else None,
                course_name=course_obj.name if course_obj else None,
                chapter_name=chapter_obj.name if chapter_obj else None,
                section_name=section_obj.name if section_obj else None,
                knowledge_point_name=kp_obj.name if kp_obj else None,
                created_by=user.id,
                status="pending",
            )
            db.add(res)
            db.flush()

            _process_resource_pipeline(res, db)

            tags = _split_tags(tags_raw)
            for t in tags:
                res.tags.append(ResourceTag(tag=t))
            res.resource_teachers.append(ResourceTeacher(teacher_id=trow.id))

            deans = db.execute(
                select(User).join(User.roles).where(Role.name == "dean")
            ).scalars().all()
            for dean in deans:
                db.add(Notification(
                    user_id=dean.id,
                    title="待审核资源",
                    content=f"教师 {user.username} 上传了新资源《{res.title}》，请及时审核。",
                    type="audit_pending",
                    related_id=res.id
                ))

            db.commit()
            db.refresh(res)
            return jsonify({"resource": _resource_dto(res)})

    @app.post("/api/resources/batch-approve-all")
    def batch_approve_all_resources():
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"dean"})

            # Find all pending resources
            pending_resources = db.execute(
                select(Resource).where(Resource.status == "pending")
            ).scalars().all()

            if not pending_resources:
                return jsonify({"ok": True, "count": 0})

            now = dt.datetime.utcnow()
            count = 0
            for res in pending_resources:
                res.status = "approved"
                res.audited_by = user.id
                res.audited_at = now
                
                # Sync with Neo4j
                # 审核通过时，同步关联的所有知识点到 Neo4j
                for rkp in res.resource_knowledge_points:
                    if rkp.knowledge_point:
                        _neo4j_upsert_kp(rkp.knowledge_point, res.course)
                # 同步资源节点及其与知识点的关系
                _neo4j_upsert_resource(res)
                
                # Notify teacher
                db.add(Notification(
                    user_id=res.created_by,
                    title="资源审核结果 (一键通过)",
                    content=f"您的资源《{res.title}》已通过系统一键审核通过。",
                    type="audit_result",
                    related_id=res.id
                ))
                count += 1

            db.commit()
            return jsonify({"ok": True, "count": count})

    @app.post("/api/resources/batch-upload")
    def batch_upload_resources():
        with SessionLocal() as db:
            try:
                user = require_auth(db)
                require_roles(user, {"teacher"})

                course_id = _parse_int(request.form.get("course_id"))
                chapter_id = _parse_int(request.form.get("chapter_id"))
                section_id = _parse_int(request.form.get("section_id"))
                
                print(f"[*] Batch upload request: course_id={course_id}, chapter_id={chapter_id}, section_id={section_id}")
                
                if course_id is None:
                    raise ApiError("BAD_REQUEST", "course_id is required", 400)

                course_obj = db.get(Course, course_id)
                if not course_obj:
                    raise ApiError("NOT_FOUND", "course not found", 404)

                chapter_obj = None
                section_obj = None

                if section_id:
                    section_obj = db.get(Section, section_id)
                    if not section_obj:
                        raise ApiError("NOT_FOUND", "section not found", 404)
                    chapter_obj = db.get(Chapter, section_obj.chapter_id)
                    if not chapter_obj or chapter_obj.course_id != course_id:
                        raise ApiError("FORBIDDEN", "section does not belong to this course", 403)
                elif chapter_id:
                    chapter_obj = db.get(Chapter, chapter_id)
                    if not chapter_obj or chapter_obj.course_id != course_id:
                        raise ApiError("NOT_FOUND", "chapter not found or does not belong to this course", 404)

                trow = db.execute(select(Teacher).where(Teacher.user_id == user.id)).scalar_one_or_none()
                if not trow:
                    raise ApiError("FORBIDDEN", "teacher profile not found", 403)

                files = request.files.getlist("files")
                print(f"[*] Files received: {len(files)}")
                if not files:
                    raise ApiError("BAD_REQUEST", "no files uploaded", 400)

                results = []
                allowed_extensions = {".docx", ".pdf", ".pptx", ".xlsx", ".txt"}
                for f in files:
                    # 使用 savepoint (nested transaction) 确保单个文件失败不影响整个批次
                    sp = db.begin_nested()
                    try:
                        original_name = (f.filename or "upload").strip()
                        print(f"[*] Processing file: {original_name}")
                        suffix = Path(original_name).suffix.lower()
                        if suffix not in allowed_extensions:
                            results.append({"filename": original_name, "status": "error", "message": f"仅支持上传 {', '.join([ext.lstrip('.') for ext in allowed_extensions])} 格式文件"})
                            sp.rollback()
                            continue

                        file_id = str(uuid.uuid4())
                        safe_name = f"{file_id}{suffix}"
                        dest_path = settings.upload_dir / safe_name
                        f.save(str(dest_path))
                        print(f"[*] Saved to: {dest_path}")

                        res = Resource(
                            title=Path(original_name).stem,
                            file_name=original_name,
                            file_path=str(dest_path),
                            file_type=suffix.lstrip("."),
                            file_size=dest_path.stat().st_size if dest_path.exists() else None,
                            course_id=course_obj.id,
                            chapter_id=chapter_obj.id if chapter_obj else None,
                            section_id=section_obj.id if section_obj else None,
                            course_name=course_obj.name,
                            chapter_name=chapter_obj.name if chapter_obj else None,
                            section_name=section_obj.name if section_obj else None,
                            created_by=user.id,
                            status="pending",
                        )
                        db.add(res)
                        db.flush()

                        _process_resource_pipeline(res, db)

                        res.resource_teachers.append(ResourceTeacher(teacher_id=trow.id))
                        results.append({
                            "filename": original_name,
                            "status": "success",
                            "kp": res.knowledge_point_name,
                            "chapter": res.chapter_name,
                            "section": res.section_name,
                            "suggestion": res.suggestion,
                            "id": res.id
                        })
                        sp.commit()
                    except Exception as fe:
                        sp.rollback()
                        print(f"[!] Error processing individual file {f.filename}: {fe}")
                        traceback.print_exc()
                        results.append({"filename": f.filename, "status": "error", "message": str(fe)})

                deans = db.execute(select(User).join(User.roles).where(Role.name == "dean")).scalars().all()
                for dean in deans:
                    db.add(Notification(
                        user_id=dean.id,
                        title="批量资源待审核",
                        content=f"教师 {user.username} 批量上传了 {len(files)} 份资源，请及时审核。",
                        type="audit_pending"
                    ))

                db.commit()
                return jsonify({"results": results})
            except ApiError as e:
                raise e
            except Exception as e:
                print(f"[!] Critical error in batch_upload_resources: {e}")
                traceback.print_exc()
                raise ApiError("INTERNAL_ERROR", f"批量处理过程中发生异常: {str(e)}", 500)

    @app.post("/api/resources/<int:resource_id>/apply-suggestion")
    def apply_resource_suggestion(resource_id: int):
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"teacher", "dean"})

            res = db.get(Resource, resource_id)
            if not res:
                raise ApiError("NOT_FOUND", "resource not found", 404)
            
            if not res.suggestion:
                raise ApiError("BAD_REQUEST", "no suggestion available for this resource", 400)
            
            s = res.suggestion
            target_id = s.get("target_id")
            target_type = s.get("target_type")

            if target_type == "course":
                res.chapter_id = None
                res.section_id = None
                res.chapter_name = None
                res.section_name = None
            elif target_type == "chapter":
                chap = db.get(Chapter, target_id)
                if chap:
                    res.chapter_id = chap.id
                    res.chapter_name = chap.name
                    res.section_id = None
                    res.section_name = None
            elif target_type == "section":
                sec = db.get(Section, target_id)
                if sec:
                    res.section_id = sec.id
                    res.section_name = sec.name
                    res.chapter_id = sec.chapter_id
                    res.chapter_name = sec.chapter.name if sec.chapter else None
            
            # 清除建议
            res.suggestion = None
            
            # 同步更新 Neo4j
            if neo4j_driver:
                _neo4j_upsert_resource(res)
                
            db.commit()
            return jsonify({"ok": True, "message": "已成功移动资源到建议位置"})

    @app.post("/api/blacklist")
    def add_to_blacklist():
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"teacher", "dean"})
            
            data = request.json or {}
            word = data.get("word", "").strip()
            course_id = _parse_int(data.get("course_id"))
            reason = data.get("reason", "教师反馈")
            
            if not word:
                raise ApiError("BAD_REQUEST", "word is required", 400)
                
            # 检查是否已存在
            existing = db.execute(
                select(Blacklist).where(Blacklist.word == word).where(Blacklist.course_id == course_id)
            ).scalar_one_or_none()
            
            if not existing:
                new_entry = Blacklist(word=word, course_id=course_id, reason=reason)
                db.add(new_entry)
                
                # 如果当前有正在处理的资源关联了该词，可以在此处触发重新处理（可选）
                
                db.commit()
            
            return jsonify({"ok": True, "message": f"已将 '{word}' 加入黑名单"})

    @app.post("/api/catalog/import")
    def import_catalog():
        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"teacher"})

            course_id = _parse_int(request.form.get("course_id"))
            if course_id is None:
                raise ApiError("BAD_REQUEST", "course_id is required", 400)

            course_obj = db.get(Course, course_id)
            if not course_obj:
                raise ApiError("NOT_FOUND", "course not found", 404)

            trow = db.execute(select(Teacher).where(Teacher.user_id == user.id)).scalar_one_or_none()
            if not trow:
                raise ApiError("FORBIDDEN", "teacher profile not found", 403)
            assigned = db.execute(
                select(CourseTeacher).where(CourseTeacher.course_id == course_id).where(CourseTeacher.teacher_id == trow.id)
            ).scalars().first()
            if not assigned:
                raise ApiError("FORBIDDEN", "not assigned to this course", 403)

            if "file" not in request.files:
                raise ApiError("BAD_REQUEST", "catalog file is required", 400)
            f = request.files["file"]
            original_name = (f.filename or "catalog.txt").strip()
            suffix = Path(original_name).suffix.lower()

            if suffix not in {".txt", ".json"}:
                raise ApiError("BAD_REQUEST", "only .txt or .json catalog files are supported", 400)

            content = f.read().decode("utf-8")

            chapters_data = []
            if suffix == ".json":
                import json
                try:
                    chapters_data = json.loads(content)
                except json.JSONDecodeError:
                    raise ApiError("BAD_REQUEST", "invalid JSON format", 400)
            else:
                lines = [l.strip() for l in content.split("\n") if l.strip()]
                current_chapter = None
                current_section = None
                for line in lines:
                    if line.startswith("# ") or line.startswith("第") and "章" in line:
                        current_chapter = line.lstrip("# ").strip()
                        current_section = None
                        chapters_data.append({"name": current_chapter, "sections": []})
                    elif line.startswith("## ") or line.startswith("第") and "节" in line:
                        current_section = line.lstrip("# ").strip()
                        if current_chapter and chapters_data:
                            chapters_data[-1]["sections"].append({"name": current_section})
                    elif line.startswith("- ") or line.startswith("* "):
                        item = line.lstrip("- *").strip()
                        if item:
                            if not current_chapter:
                                current_chapter = item
                                chapters_data.append({"name": current_chapter, "sections": []})
                            elif current_section is None:
                                current_section = item
                                chapters_data[-1]["sections"].append({"name": current_section})
                            else:
                                chapters_data[-1]["sections"][-1]["subsections"] = chapters_data[-1]["sections"][-1].get("subsections", [])
                                chapters_data[-1]["sections"][-1]["subsections"].append(item)

            created_chapters = []
            created_sections = []

            for idx, ch_data in enumerate(chapters_data):
                chapter_name = ch_data.get("name", "").strip()
                if not chapter_name:
                    continue

                existing_chapter = db.execute(
                    select(Chapter).where(Chapter.course_id == course_id).where(Chapter.name == chapter_name)
                ).scalar_one_or_none()

                if not existing_chapter:
                    existing_chapter = Chapter(name=chapter_name, course_id=course_id, order_index=idx)
                    db.add(existing_chapter)
                    db.flush()
                    _neo4j_upsert_chapter(existing_chapter, course_name=course_obj.name)
                    created_chapters.append({"id": existing_chapter.id, "name": existing_chapter.name})
                else:
                    _neo4j_upsert_chapter(existing_chapter, course_name=course_obj.name)
                    created_chapters.append({"id": existing_chapter.id, "name": existing_chapter.name})

                sections_data = ch_data.get("sections", [])
                for sidx, sec_data in enumerate(sections_data):
                    section_name = sec_data.get("name", "").strip()
                    if not section_name:
                        continue

                    existing_section = db.execute(
                        select(Section).where(Section.chapter_id == existing_chapter.id).where(Section.name == section_name)
                    ).scalar_one_or_none()

                    if not existing_section:
                        existing_section = Section(name=section_name, chapter_id=existing_chapter.id, order_index=sidx)
                        db.add(existing_section)
                        db.flush()
                        _neo4j_upsert_section(existing_section, chapter_id=existing_chapter.id)
                        created_sections.append({"id": existing_section.id, "name": existing_section.name, "chapter_id": existing_chapter.id})
                    else:
                        _neo4j_upsert_section(existing_section, chapter_id=existing_chapter.id)

            db.commit()

            return jsonify({
                "ok": True,
                "course_id": course_id,
                "chapters_count": len(created_chapters),
                "sections_count": len(created_sections),
                "chapters": created_chapters,
                "sections": created_sections
            })

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
                pass
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
                # approved resources are visible to everyone
                pass

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
                # approved resources can be downloaded by everyone
                pass
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
            result = _delete_resource_with_relations(db, res)
            db.commit()
            return jsonify({"ok": True, "result": result})

    @app.post("/api/resources/batch-delete")
    def batch_delete_resources():
        data = _json()
        ids = data.get("ids")
        if not isinstance(ids, list) or not all(isinstance(x, int) for x in ids):
            raise ApiError("BAD_REQUEST", "ids must be int[]", 400)

        ids = [int(x) for x in ids]
        ids = list(dict.fromkeys(ids))
        if not ids:
            return jsonify({"ok": True, "results": [], "deleted": {}})
        if len(ids) > 500:
            raise ApiError("BAD_REQUEST", "too many ids", 400)

        with SessionLocal() as db:
            user = require_auth(db)
            require_roles(user, {"dean", "admin"})

            results: List[Dict[str, Any]] = []
            totals = {"resources": 0, "resource_files": 0, "behaviors": 0, "notifications": 0, "resource_teachers": 0, "tags": 0}

            for rid in ids:
                res = db.get(Resource, rid)
                if not res:
                    results.append({"resource_id": rid, "status": "not_found"})
                    continue
                try:
                    out = _delete_resource_with_relations(db, res)
                    d = out.get("deleted") or {}
                    totals["resources"] += int(d.get("resource") or 0)
                    totals["resource_files"] += int(d.get("resource_file") or 0)
                    totals["behaviors"] += int(d.get("behaviors") or 0)
                    totals["notifications"] += int(d.get("notifications") or 0)
                    totals["resource_teachers"] += int(d.get("resource_teachers") or 0)
                    totals["tags"] += int(d.get("tags") or 0)
                    results.append({"resource_id": rid, "status": "deleted", "deleted": d})
                except Exception as e:
                    results.append({"resource_id": rid, "status": "error", "message": str(e)})

            db.commit()
            return jsonify({"ok": True, "results": results, "deleted": totals})

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

            if res.status == "approved":
                # 审核通过时，同步关联的所有知识点到 Neo4j
                for rkp in res.resource_knowledge_points:
                    if rkp.knowledge_point:
                        _neo4j_upsert_kp(rkp.knowledge_point, res.course)
                # 同步资源节点及其与知识点的关系
                _neo4j_upsert_resource(res)

            # Notify teacher
            status_text = "通过" if status == "approved" else "拒绝"
            db.add(Notification(
                user_id=res.created_by,
                title="资源审核结果",
                content=f"您的资源《{res.title}》已被审核，结果为：{status_text}。审核意见：{comment or '无'}",
                type="audit_result",
                related_id=res.id
            ))

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
            
            # 检查是否已存在该行为
            exists = db.execute(
                select(UserBehavior)
                .where(UserBehavior.user_id == user.id)
                .where(UserBehavior.resource_id == resource_id)
                .where(UserBehavior.action == action)
            ).scalar_one_or_none()
            
            if not exists:
                db.add(UserBehavior(user_id=user.id, resource_id=resource_id, action=action))
                db.commit()
                
                # 如果是收藏操作，触发智能推送机制
                if action == "favorite":
                    _trigger_smart_push(db, user.id, neo4j_driver)
            
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
        level = (request.args.get("level") or "").strip().lower() or "departments"
        if level not in {"full", "courses", "departments"}:
            level = "departments"
        with SessionLocal() as db:
            current = get_current_user(db)
            
            if neo4j_driver:
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
                    # Fallback to approved resources
                    q2 = select(Resource.course_name).where(Resource.status == "approved").order_by(Resource.created_at.desc())
                    if course:
                        q2 = q2.where(Resource.course_name == course)
                    
                    for name in db.execute(q2).scalars().all():
                        if name:
                            upsert_node(f"course:{name}", name, "course")

                return jsonify({"nodes": list(nodes.values()), "links": [], "source": "mysql"})

            q = select(Resource).where(Resource.status == "approved").order_by(Resource.created_at.desc())
            if course:
                q = q.where((Resource.course_name == course))
            
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
            return jsonify({"ok": True, "via": via, "items": [_resource_dto(r) for r in db.execute(stmt).scalars().all()]})

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

    def _user_admin_dto(u: User, db: Session) -> Dict[str, Any]:
        data = {
            "id": u.id,
            "username": u.username,
            "roles": [r.name for r in u.roles],
            "is_active": bool(u.is_active),
            "created_at": u.created_at.isoformat() if u.created_at else None,
        }
        roles = {r.name for r in u.roles}
        if "student" in roles:
            s = db.execute(select(Student).where(Student.user_id == u.id)).scalar_one_or_none()
            if s:
                data["class_name"] = s.class_name
        return data

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
            return jsonify({"items": [_user_admin_dto(u, db) for u in users]})

    @app.post("/api/admin/users")
    def admin_create_user():
        data = _json()
        username = str(data.get("username", "")).strip()
        password = str(data.get("password", "")).strip()
        roles_raw = data.get("roles")
        is_active = data.get("is_active")
        class_name = data.get("class_name")
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
            
            # Update student class_name if applicable
            if "student" in [rr.name for rr in new_user.roles]:
                s = db.execute(select(Student).where(Student.user_id == new_user.id)).scalar_one_or_none()
                if s:
                    s.class_name = class_name
            
            db.commit()
            db.refresh(new_user)
            return jsonify({"user": _user_admin_dto(new_user, db)})

    @app.post("/api/admin/users/bulk-import")
    def admin_bulk_import_users():
        if 'file' not in request.files:
            raise ApiError("BAD_REQUEST", "no file uploaded", 400)
        
        file = request.files['file']
        if not file or not file.filename:
            raise ApiError("BAD_REQUEST", "empty file", 400)
        
        import pandas as pd
        import io

        try:
            if file.filename.endswith('.csv'):
                df = pd.read_csv(io.BytesIO(file.read()))
            elif file.filename.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(io.BytesIO(file.read()))
            else:
                raise ApiError("BAD_REQUEST", "unsupported file format, use CSV or Excel", 400)
        except Exception as e:
            raise ApiError("BAD_REQUEST", f"failed to parse file: {str(e)}", 400)

        # Expected columns: username, password, roles (comma separated), class_name (optional)
        # Validate columns
        required_cols = {'username', 'password', 'roles'}
        if not required_cols.issubset(df.columns):
            raise ApiError("BAD_REQUEST", f"missing required columns: {required_cols - set(df.columns)}", 400)

        results = {"success": 0, "failed": 0, "errors": []}
        
        with SessionLocal() as db:
            admin = require_auth(db)
            require_roles(admin, {"admin", "dean"})
            admin_roles = {r.name for r in admin.roles}
            is_dean = ("dean" in admin_roles) and ("admin" not in admin_roles)

            role_cache = {r.name: r for r in db.execute(select(Role)).scalars().all()}

            for index, row in df.iterrows():
                try:
                    username = str(row['username']).strip()
                    password = str(row['password']).strip()
                    roles_str = str(row['roles']).strip()
                    class_name = str(row.get('class_name', '')).strip() if 'class_name' in df.columns else None

                    if not username or not password or not roles_str:
                        raise ValueError("username, password, and roles are required")

                    roles_in = [r.strip() for r in roles_str.replace('，', ',').split(',') if r.strip()]
                    
                    if is_dean:
                        if set(roles_in).difference({"teacher", "student"}):
                            raise ValueError(f"dean can only create teacher or student accounts (row {index+2})")

                    # Check if user exists
                    existing = db.execute(select(User).where(User.username == username)).scalar_one_or_none()
                    if existing:
                        raise ValueError(f"username '{username}' already exists")

                    # Create user
                    new_user = User(
                        username=username,
                        password_hash=generate_password_hash(password),
                        is_active=True
                    )
                    db.add(new_user)
                    db.flush()

                    for rname in roles_in:
                        if rname not in role_cache:
                            raise ValueError(f"invalid role: {rname}")
                        new_user.roles.append(role_cache[rname])
                    
                    _ensure_user_profiles(db, new_user, roles_in)
                    
                    if "student" in roles_in and class_name:
                        s = db.execute(select(Student).where(Student.user_id == new_user.id)).scalar_one_or_none()
                        if s:
                            s.class_name = class_name
                    
                    results["success"] += 1
                except Exception as e:
                    results["failed"] += 1
                    results["errors"].append(f"Row {index+2}: {str(e)}")
            
            db.commit()

        return jsonify(results)

    @app.patch("/api/admin/users/<int:user_id>")
    def admin_update_user(user_id: int):
        data = _json()
        roles_raw = data.get("roles")
        is_active = data.get("is_active")
        password = data.get("password")
        class_name = data.get("class_name")

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

            if class_name is not None:
                s = db.execute(select(Student).where(Student.user_id == u.id)).scalar_one_or_none()
                if s:
                    s.class_name = class_name

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
            return jsonify({"user": _user_admin_dto(u, db)})

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
    chapter_label = r.chapter.name if r.chapter else r.chapter_name
    section_label = r.section.name if r.section else r.section_name
    kp_label = r.knowledge_point.name if r.knowledge_point else r.knowledge_point_name
    kps = []
    if r.resource_knowledge_points:
        for rkp in r.resource_knowledge_points:
            if rkp.knowledge_point:
                kps.append({"id": rkp.knowledge_point.id, "name": rkp.knowledge_point.name})
    if not kps and kp_label:
        # 兼容旧逻辑或冗余字段
        for name in [n.strip() for n in kp_label.split(",") if n.strip()]:
            kps.append({"id": r.knowledge_point_id, "name": name})

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
        "chapter_id": r.chapter_id,
        "section_id": r.section_id,
        "knowledge_point_id": r.knowledge_point_id,
        "course": course_label,
        "chapter": chapter_label,
        "section": section_label,
        "knowledge_point": kp_label,
        "knowledge_points": kps,
        "teachers": teachers,
        "status": r.status,
        "audit_comment": r.audit_comment,
        "audited_by": r.audited_by,
        "audited_at": r.audited_at.isoformat() if r.audited_at else None,
        "tags": [t.tag for t in r.tags],
        "suggestion": r.suggestion,
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "updated_at": r.updated_at.isoformat() if r.updated_at else None,
    }


def _neo4j_overview(driver, course: Optional[str], level: str) -> Dict[str, Any]:
    if level == "departments":
        nodes: Dict[str, Dict[str, Any]] = {}
        with driver.session() as session:
            records = session.run("MATCH (d:Department) RETURN d.name as name").data()
        for rec in records:
            name = rec["name"]
            nodes[f"dept:{name}"] = {"id": f"dept:{name}", "label": name, "type": "department"}
        return {"nodes": list(nodes.values()), "links": [], "source": "neo4j"}

    if level == "courses":
        nodes: Dict[str, Dict[str, Any]] = {}
        links: List[Dict[str, Any]] = []

        def upsert_node(node_id: str, label: str, ntype: str) -> None:
            nodes.setdefault(node_id, {"id": node_id, "label": label, "type": ntype})

        with driver.session() as session:
            # Fetch Courses and Departments
            records = (
                session.run(
                    """
                    MATCH (c:Course)
                    WHERE ($course IS NULL OR c.name = $course)
                    OPTIONAL MATCH (c)-[:BELONGS_TO_DEPT]->(d:Department)
                    RETURN DISTINCT c.name as course, d.name as dept
                    """,
                    {"course": course},
                ).data()
                or []
            )
        for rec in records:
            c = rec.get("course")
            d = rec.get("dept")
            if not c:
                continue
            upsert_node(f"course:{c}", c, "course")
            if d:
                upsert_node(f"dept:{d}", d, "department")
                links.append({"source": f"course:{c}", "target": f"dept:{d}", "type": "belongs_to"})
        return {"nodes": list(nodes.values()), "links": links, "source": "neo4j"}

    if level == "full":
        query = """
        MATCH (c:Course)
        WHERE ($course IS NULL OR c.name = $course)
        OPTIONAL MATCH (c)-[:BELONGS_TO_MAJOR]->(m:Major)
        OPTIONAL MATCH (m)-[:BELONGS_TO_DEPT]->(d:Department)
        OPTIONAL MATCH (t:Teacher)-[:TEACHES]->(c)
        OPTIONAL MATCH (c)-[:HAS_CHAPTER]->(ch:Chapter)
        OPTIONAL MATCH (ch)-[:HAS_SECTION]->(s:Section)
        OPTIONAL MATCH (s)-[:HAS_KP]->(k:KnowledgePoint)
        OPTIONAL MATCH (c)-[:HAS_KP]->(k2:KnowledgePoint)
        OPTIONAL MATCH (k)-[:RELATED_RESOURCE]->(r:Resource)
        OPTIONAL MATCH (k2)-[:RELATED_RESOURCE]->(r2:Resource)
        RETURN c.name as course, m.name as major, d.name as dept, 
               t.name as teacher,
               ch.id as chid, ch.name as chname,
               s.id as sid, s.name as sname,
               k.name as kp, k2.name as kp2,
               collect(distinct r.id) as res_ids, collect(distinct r2.id) as res_ids2
        """
        nodes: Dict[str, Dict[str, Any]] = {}
        links: List[Dict[str, Any]] = []

        def upsert_node(node_id: str, label: str, ntype: str) -> None:
            nodes.setdefault(node_id, {"id": node_id, "label": label, "type": ntype})

        with driver.session() as session:
            records = session.run(query, {"course": course}).data()
        
        for rec in records:
            c = rec.get("course")
            if not c: continue
            upsert_node(f"course:{c}", c, "course")
            
            m = rec.get("major")
            if m:
                upsert_node(f"major:{m}", m, "major")
                links.append({"source": f"course:{c}", "target": f"major:{m}", "type": "belongs_to_major"})
            
            d = rec.get("dept")
            if d:
                upsert_node(f"dept:{d}", d, "department")
                if m:
                    links.append({"source": f"major:{m}", "target": f"dept:{d}", "type": "belongs_to_dept"})
                else:
                    links.append({"source": f"course:{c}", "target": f"dept:{d}", "type": "belongs_to_dept"})
            
            t = rec.get("teacher")
            if t:
                upsert_node(f"teacher:{t}", t, "teacher")
                links.append({"source": f"teacher:{t}", "target": f"course:{c}", "type": "teaches"})
            
            chid = rec.get("chid")
            chname = rec.get("chname")
            if chid and chname:
                upsert_node(f"chapter:{chid}", chname, "chapter")
                links.append({"source": f"course:{c}", "target": f"chapter:{chid}", "type": "has_chapter"})
                
                sid = rec.get("sid")
                sname = rec.get("sname")
                if sid and sname:
                    upsert_node(f"section:{sid}", sname, "section")
                    links.append({"source": f"chapter:{chid}", "target": f"section:{sid}", "type": "has_section"})
                    
                    k = rec.get("kp")
                    if k:
                        upsert_node(f"kp:{k}", k, "knowledge_point")
                        links.append({"source": f"section:{sid}", "target": f"kp:{k}", "type": "has_kp"})
                        for rid in rec.get("res_ids") or []:
                            upsert_node(f"res:{rid}", str(rid), "resource")
                            links.append({"source": f"kp:{k}", "target": f"res:{rid}", "type": "related_resource"})
            
            k2 = rec.get("kp2")
            if k2:
                upsert_node(f"kp:{k2}", k2, "knowledge_point")
                links.append({"source": f"course:{c}", "target": f"kp:{k2}", "type": "has_kp"})
                for rid in rec.get("res_ids2") or []:
                    upsert_node(f"res:{rid}", str(rid), "resource")
                    links.append({"source": f"kp:{k2}", "target": f"res:{rid}", "type": "related_resource"})
        
        return {"nodes": list(nodes.values()), "links": links, "source": "neo4j"}

    # Default to courses level if not specified or unrecognized
    return _neo4j_overview(driver, course, "courses")


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

            # Link to Section if exists
            for row in (
                session.run(
                    """
                    MATCH (s:Section)-[:HAS_KP]->(k:KnowledgePoint {name: $name})
                    RETURN DISTINCT s.id as sid, s.name as sname
                    """,
                    {"name": raw},
                ).data()
                or []
            ):
                sid = row.get("sid")
                sname = row.get("sname")
                if sid and sname:
                    snid = f"section:{sid}"
                    upsert_node(snid, sname, "section")
                    add_link(snid, center_id, "has_kp")

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
                UNION
                MATCH (k:KnowledgePoint {{name: $name}})<-[:HAS_KP]-(s:Section)-[:HAS_RESOURCE]->(r:Resource)
                RETURN 'section' as src_type, s.id as src, r.id as rid
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
                elif src_type == "section":
                    sid = f"section:{src}"
                    # We might need the section name here, but for now we just link
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

    elif node_type == "chapter":
        try:
            cid_int = int(raw)
        except Exception:
            return {"nodes": [], "links": [], "resource_ids": [], "paths": {}}
        center_id = f"chapter:{cid_int}"
        with driver.session() as session:
            ch_row = session.run("MATCH (ch:Chapter {id: $id}) RETURN ch.name as name", {"id": cid_int}).single()
            if not ch_row:
                return {"nodes": [], "links": [], "resource_ids": [], "paths": {}}
            ch_name = ch_row["name"]
            upsert_node(center_id, ch_name, "chapter")
            
            # Show parent course
            course_rows = session.run("MATCH (c:Course)-[:HAS_CHAPTER]->(ch:Chapter {id: $id}) RETURN c.name as name", {"id": cid_int}).data()
            for row in course_rows:
                cname = row["name"]
                coid = f"course:{cname}"
                upsert_node(coid, cname, "course")
                add_link(coid, center_id, "has_chapter")
                
            # Show child sections
            sec_rows = session.run("MATCH (ch:Chapter {id: $id})-[:HAS_SECTION]->(s:Section) RETURN s.id as sid, s.name as sname", {"id": cid_int}).data()
            for row in sec_rows:
                sid = row["sid"]
                sname = row["sname"]
                snid = f"section:{sid}"
                upsert_node(snid, sname, "section")
                add_link(center_id, snid, "has_section")

    elif node_type == "section":
        try:
            sid_int = int(raw)
        except Exception:
            return {"nodes": [], "links": [], "resource_ids": [], "paths": {}}
        center_id = f"section:{sid_int}"
        with driver.session() as session:
            s_row = session.run("MATCH (s:Section {id: $id}) RETURN s.name as name", {"id": sid_int}).single()
            if not s_row:
                return {"nodes": [], "links": [], "resource_ids": [], "paths": {}}
            s_name = s_row["name"]
            upsert_node(center_id, s_name, "section")
            
            # Show parent chapter
            ch_rows = session.run("MATCH (ch:Chapter)-[:HAS_SECTION]->(s:Section {id: $id}) RETURN ch.id as chid, ch.name as chname", {"id": sid_int}).data()
            for row in ch_rows:
                chid = row["chid"]
                chname = row["chname"]
                chnid = f"chapter:{chid}"
                upsert_node(chnid, chname, "chapter")
                add_link(chnid, center_id, "has_section")
                
            # Show child knowledge points
            kp_rows = session.run("MATCH (s:Section {id: $id})-[:HAS_KP]->(k:KnowledgePoint) RETURN k.name as kname", {"id": sid_int}).data()
            for row in kp_rows:
                kname = row["kname"]
                knid = f"kp:{kname}"
                upsert_node(knid, kname, "knowledge_point")
                add_link(center_id, knid, "has_kp")

    elif node_type == "dept":
        center_id = f"dept:{raw}"
        upsert_node(center_id, raw, "department")
        with driver.session() as session:
            major_rows = (
                session.run(
                    """
                    MATCH (d:Department {name: $name})<-[:BELONGS_TO_DEPT]-(m:Major)
                    RETURN DISTINCT m.name as major
                    """,
                    {"name": raw},
                ).data()
                or []
            )
            for row in major_rows:
                m = row.get("major")
                if not m:
                    continue
                mid = f"major:{m}"
                upsert_node(mid, m, "major")
                add_link(mid, center_id, "belongs_to_dept")

    elif node_type == "major":
        center_id = f"major:{raw}"
        upsert_node(center_id, raw, "major")
        with driver.session() as session:
            # Add Department link
            dept_rows = session.run("MATCH (m:Major {name: $name})-[:BELONGS_TO_DEPT]->(d:Department) RETURN d.name as dept", {"name": raw}).data()
            for row in dept_rows:
                d = row["dept"]
                did = f"dept:{d}"
                upsert_node(did, d, "department")
                add_link(center_id, did, "belongs_to_dept")
            
            # Add Course links
            course_rows = session.run("MATCH (m:Major {name: $name})<-[:BELONGS_TO_MAJOR]-(c:Course) RETURN c.name as course", {"name": raw}).data()
            for row in course_rows:
                c = row["course"]
                cid = f"course:{c}"
                upsert_node(cid, c, "course")
                add_link(cid, center_id, "belongs_to_major")

    elif node_type == "course":
        center_id = f"course:{raw}"
        upsert_node(center_id, raw, "course")
        with driver.session() as session:
            # Add Major link
            major_rows = session.run("MATCH (c:Course {name: $name})-[:BELONGS_TO_MAJOR]->(m:Major) RETURN m.name as major", {"name": raw}).data()
            for row in major_rows:
                m = row["major"]
                mid = f"major:{m}"
                upsert_node(mid, m, "major")
                add_link(center_id, mid, "belongs_to_major")
            
            # Add Teacher links
            teacher_rows = session.run("MATCH (t:Teacher)-[:TEACHES]->(c:Course {name: $name}) RETURN t.name as teacher", {"name": raw}).data()
            for row in teacher_rows:
                t = row["teacher"]
                tid = f"teacher:{t}"
                upsert_node(tid, t, "teacher")
                add_link(tid, center_id, "teaches")

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

            # Add Chapter link
            chapter_rows = (
                session.run(
                    """
                    MATCH (c:Course {name: $name})-[:HAS_CHAPTER]->(ch:Chapter)
                    RETURN DISTINCT ch.id as cid, ch.name as cname
                    """,
                    {"name": raw},
                ).data()
                or []
            )
            for row in chapter_rows:
                cid = row.get("cid")
                cname = row.get("cname")
                if cid and cname:
                    cnid = f"chapter:{cid}"
                    upsert_node(cnid, cname, "chapter")
                    add_link(center_id, cnid, "has_chapter")

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
        if kp:
            if kp.course:
                cid = f"course:{kp.course.name}"
                upsert_node(cid, kp.course.name, "course")
                add_link(cid, center_id, "has_kp")
            if kp.section:
                sid = f"section:{kp.section_id}"
                upsert_node(sid, kp.section.name, "section")
                add_link(sid, center_id, "has_kp")
            elif kp.chapter:
                chid = f"chapter:{kp.chapter_id}"
                upsert_node(chid, kp.chapter.name, "chapter")
                add_link(chid, center_id, "has_kp")

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
            # Show Chapters
            chapters = db.execute(select(Chapter).where(Chapter.course_id == c.id).order_by(Chapter.order_index.asc())).scalars().all()
            for ch in chapters:
                chid = f"chapter:{ch.id}"
                upsert_node(chid, ch.name, "chapter")
                add_link(center_id, chid, "has_chapter")
                
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
                # 只有当知识点既没有章节也没有小节时，才直接连到课程
                if not k.chapter_id and not k.section_id:
                    add_link(center_id, kid, "has_kp")
        if expand == "kps":
            return {"nodes": list(nodes.values()), "links": links, "resource_ids": [], "paths": {}}
        q = select(Resource).where(Resource.status == "approved").order_by(Resource.created_at.desc())
        if c:
            q = q.where((Resource.course_id == c.id) | (Resource.course_name == raw))
        else:
            q = q.where(Resource.course_name == raw)
        for r in db.execute(q).scalars().all():
            # 只有当资源既没有章节也没有小节时，才在探索视图中直接连到课程
            if not r.chapter_id and not r.section_id:
                add_resource(r, via_course=raw)
            else:
                # 否则只记录资源 ID，但不建立与课程的直接 Link
                nodes[f"res:{r.id}"] = {"id": f"res:{r.id}", "label": r.title, "type": "resource"}
                resource_ids.append(r.id)

    elif node_type == "chapter":
        try:
            cid = int(raw)
        except Exception:
            return {"nodes": [], "links": [], "resource_ids": [], "paths": {}}
        ch = db.get(Chapter, cid)
        if not ch:
            return {"nodes": [], "links": [], "resource_ids": [], "paths": {}}
        center_id = f"chapter:{cid}"
        upsert_node(center_id, ch.name, "chapter")
        if ch.course:
            coid = f"course:{ch.course.name}"
            upsert_node(coid, ch.course.name, "course")
            add_link(coid, center_id, "has_chapter")
        for s in ch.sections:
            sid = f"section:{s.id}"
            upsert_node(sid, s.name, "section")
            add_link(center_id, sid, "has_section")

    elif node_type == "section":
        try:
            sid = int(raw)
        except Exception:
            return {"nodes": [], "links": [], "resource_ids": [], "paths": {}}
        s = db.get(Section, sid)
        if not s:
            return {"nodes": [], "links": [], "resource_ids": [], "paths": {}}
        center_id = f"section:{sid}"
        upsert_node(center_id, s.name, "section")
        if s.chapter:
            chid = f"chapter:{s.chapter_id}"
            upsert_node(chid, s.chapter.name, "chapter")
            add_link(chid, center_id, "has_section")
        for k in s.knowledge_points:
            kid = f"kp:{k.name}"
            upsert_node(kid, k.name, "knowledge_point")
            add_link(center_id, kid, "has_kp")

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
    # 1. 获取用户最近收藏/学习的资源
    favored = (
        db.execute(
            select(UserBehavior.resource_id)
            .where(UserBehavior.user_id == user_id)
            .where(UserBehavior.action == "favorite")
            .order_by(UserBehavior.created_at.desc())
            .limit(20)
        )
        .scalars()
        .all()
    )
    favored_set = set(favored)

    # 2. 获取用户兴趣知识点
    kp_names = (
        db.execute(
            select(KnowledgePoint.name)
            .join(Resource, Resource.knowledge_point_id == KnowledgePoint.id)
            .where(Resource.id.in_(favored))
        )
        .scalars()
        .all()
    )
    user_interest_kps = {kp for kp in kp_names if kp}

    # 3. 利用 Neo4j 扩展语义关联知识点
    semantic_related_kps = {} # kp_name -> weight (0.1 - 1.0)
    if neo4j_driver and user_interest_kps:
        with neo4j_driver.session() as session:
            # 查找直接关联或前置/后继知识点
            query = """
            MATCH (k1:KnowledgePoint)-[r:RELATED|PREREQUISITE*1..2]-(k2:KnowledgePoint)
            WHERE k1.name IN $interests AND k1.name <> k2.name
            RETURN k2.name as name, type(r[0]) as rel_type, length(r) as dist
            """
            rows = session.run(query, {"interests": list(user_interest_kps)}).data()
            for row in rows:
                name = row["name"]
                dist = row["dist"]
                rel = row["rel_type"]
                # 距离越近、关系越强，权重越高
                weight = 0.8 if dist == 1 else 0.4
                if rel == "PREREQUISITE": weight += 0.1
                semantic_related_kps[name] = max(semantic_related_kps.get(name, 0), weight)

    # 4. 候选资源评分
    candidates = db.execute(select(Resource).where(Resource.status == "approved")).scalars().all()
    scored: List[Tuple[float, Resource, List[str]]] = []

    for r in candidates:
        if r.id in favored_set: continue
        
        score = 0.0
        reasons = []
        kp_name = r.knowledge_point.name if r.knowledge_point else r.knowledge_point_name
        
        # 维度 A: 直接知识点匹配 (最高权重)
        if kp_name and kp_name in user_interest_kps:
            score += 10.0
            reasons.append(f"基于你的学习兴趣：{kp_name}")
            
        # 维度 B: 知识图谱语义扩展 (中等权重)
        elif kp_name and kp_name in semantic_related_kps:
            weight = semantic_related_kps[kp_name]
            score += 8.0 * weight
            reasons.append(f"图谱发现：与你关注的知识点语义高度相关")

        # 维度 C: 标签重合度 (辅助权重)
        tag_set = {t.tag for t in r.tags}
        common_tags = tag_set.intersection(user_interest_kps)
        if common_tags:
            score += 2.0 * len(common_tags)
            reasons.append(f"发现相似标签：{', '.join(list(common_tags)[:2])}")

        if score > 0:
            scored.append((score, r, reasons))

    # 5. 排序并返回
    scored.sort(key=lambda x: (-x[0], -x[1].id))
    return [{"resource": _resource_dto(r), "reasons": reasons} for _, r, reasons in scored[:10]]

def _trigger_smart_push(db: Session, user_id: int, neo4j_driver):
    """智能推送触发：发现高质量关联资源并发送通知"""
    recommendations = _recommend(db, user_id, neo4j_driver)
    if not recommendations:
        return
    
    top_pick = recommendations[0]
    res_obj = top_pick["resource"]
    reason = top_pick["reasons"][0] if top_pick["reasons"] else "根据你的学习进度推荐"
    
    # 检查是否已经推送过该资源的通知，避免重复骚扰
    exists = db.execute(
        select(Notification)
        .where(Notification.user_id == user_id)
        .where(Notification.related_id == res_obj["id"])
        .where(Notification.type == "smart_push")
    ).first()
    
    if not exists:
        db.add(Notification(
            user_id=user_id,
            title="✨ 智能发现：你可能感兴趣的资源",
            content=f"系统基于知识图谱为你发现了一份优质资源《{res_obj['title']}》，{reason}。",
            type="smart_push",
            related_id=res_obj["id"]
        ))
        db.commit()


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
