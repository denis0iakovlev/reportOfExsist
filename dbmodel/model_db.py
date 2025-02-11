from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from sqlalchemy import Integer, String,DateTime,ForeignKey,Boolean, create_engine
from datetime import datetime

# Базовый класс для SQLAlchemy
class Base(DeclarativeBase):
    pass

# Определение таблицы Department
class Department(Base):
    __tablename__ = 'Department'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)

class GateType(Base):
    __tablename__ = "GateType"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    gateTypeName: Mapped[str] = mapped_column(String, unique=True, nullable=False)

class Gate(Base):
    __tablename__ = 'Gate'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    gateTypeId:Mapped[int] = mapped_column(Integer, ForeignKey('GateType.id'), nullable=False)

class Person(Base):
    __tablename__ = "Person"
    id:Mapped[int] = mapped_column(Integer, primary_key=True)
    firstName:Mapped[str] = mapped_column(String,default="Имя", nullable=True)
    lastName:Mapped[str] = mapped_column(String, default='Фамилия', nullable=True)
#Тип прохода выход/вход
class PassType(Base):
    __tablename__ = 'PickType'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    typeName: Mapped[str] = mapped_column(String, unique=True, nullable=False)

class GatePassTime(Base):
    __tablename__ = 'GatePassTime'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[DateTime] = mapped_column(DateTime,  nullable=False)
    isHoliday:Mapped[Boolean] = mapped_column(Boolean, nullable=False)
    pick_type_id: Mapped[int] = mapped_column(Integer, ForeignKey('PickType.id'), nullable=False)
    person_id: Mapped[int] = mapped_column(Integer, ForeignKey('Person.id'), nullable=False)
    gate_id: Mapped[int] = mapped_column(Integer, ForeignKey('Gate.id'), nullable=False)

# Создаем соединение с базой данных SQLite
DATABASE_URL = "sqlite:///database.db"
engine = create_engine(DATABASE_URL, echo=False) 
# Создаем таблицы
Base.metadata.create_all(engine)
# Создаем сессию
SessionLocal = sessionmaker(bind=engine)
