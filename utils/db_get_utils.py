from dbmodel.model_db import SessionLocal , Department, Gate, GateType, PassType, Person, GatePassTime
from datetime import datetime
from sqlalchemy import extract


def get_filtered_gate_pass_times(month: int = None, isHolidays: bool = None, id:int=None):
    session = SessionLocal()
    query = (
        session.query(GatePassTime, PassType.typeName)
        .join(Gate, GatePassTime.gate_id == Gate.id)
        .join(GateType, Gate.gateTypeId == GateType.id)
        .join(PassType, GatePassTime.pick_type_id == PassType.id)
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
    return [{
            "id": record.id,
            "isHoliday":record.isHoliday,
            "timestamp": record.timestamp.strftime("%Y-%m-%d %H:%M"),
            "pass_type": type_name,
            "pass_type_id": record.pick_type_id,
            "person_id": record.person_id,
            "gate_id": record.gate_id
        } 
        for record, type_name in results
        ]
