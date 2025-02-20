from dbmodel.model_db import SessionLocal , Department, Gate, GateType, PassType, Person, GatePassTime
from datetime import datetime
from sqlalchemy import extract


def get_filtered_gate_pass_times(month: int = None, isHolidays: bool = None, id:int=None):
    session = SessionLocal()
    query = (
        session.query(GatePassTime, PassType.typeName, Person.dep_name, Person.prof_name)
        .join(Gate, GatePassTime.gate_id == Gate.id)
        .join(GateType, Gate.gateTypeId == GateType.id)
        .join(PassType, GatePassTime.pick_type_id == PassType.id)
        .join(Person, GatePassTime.person_id == Person.id)
        .filter(
            GateType.gateTypeName == "ВНЕШНИЙ",
        )
    )
    if month is not None:
        query = query.filter( extract('month', GatePassTime.timestamp) == month)
    if isHolidays is not None:
        query = query.filter( GatePassTime.isHoliday == isHolidays)
    if id is not None:
        query = query.filter( GatePassTime.person_id == id)
    results = query.order_by(GatePassTime.timestamp.asc()).all()
    session.close()
    return [{
            "id": record.id,
            "isHoliday":record.isHoliday,
            "timestamp": record.timestamp.strftime("%Y-%m-%d %H:%M"),
            "pass_type": type_name,
            "pass_type_id": record.pick_type_id,
            "person_id": record.person_id,
            "gate_id": record.gate_id,
            "prof_name":prof_name,
            "dep_name":dep_name
        } 
        for record, type_name, dep_name, prof_name in results
        ]


def get_persons():
    session = SessionLocal()
    res = session.query(Person).all()
    session.close()
    return [ {col.name: getattr(rec, col.name) for col in Person.__table__.columns} for rec in res]