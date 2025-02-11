from dbmodel.model_db import SessionLocal , Department, Gate, GateType, PassType, Person, GatePassTime
from datetime import datetime
from utils.holidays_ru import get_holidays_days

def add_departments(department_names: list[str]):
    session = SessionLocal()
    try:
        for name in department_names:
            name = name.upper()
            departement = session.query(Department).filter(Department.name == name).first()
            if departement is None:
                department = Department(name=name)
                session.add(department)
        session.commit()
    except Exception as e:
        print(f"Не удалось добавить записи в таблицу Департамент. {e}")
    finally:
        session.close()

def add_gates(gate_data: list[tuple[int, str, str]]):
    session = SessionLocal()
    try:
        for id, gate_name, gate_type_name in gate_data:
            gate_type_name = gate_type_name.upper()
            gate_name = gate_name.upper()
            gate_type = session.query(GateType).filter(GateType.gateTypeName == gate_type_name).first()
            if gate_type is None:
                raise f"Отсутствует тип проходной {gate_type_name}"
            exsist_gate = session.query(Gate).filter(Gate.name == gate_name).first()
            if exsist_gate is None:
                gate = Gate(name=gate_name,gateTypeId=gate_type.id, id=id)
                session.add(gate)
        session.commit()
    except Exception as e:
        print(f"Не удалось добавить записи в таблицу Проходные. {e}")
    finally:
        session.close()

def add_gate_pass_registrations(pass_reg_list:list[tuple[str, int, str, str, str]]):
    session = SessionLocal()
    try:
        holidays = get_holidays_days()
        for department_name,personId,date, pass_type_name, gate_name in pass_reg_list:
            try:
                department_name = department_name.upper()
                department = session.query(Department).filter(Department.name ==department_name ).first()
                if department is None:
                    raise Exception(f"В переданной записи регистрации на проходной ошибка имени департамениа. Департамент {department_name} - отсутствует в БД")
                #Найти сотрудника
                person = session.query(Person).filter(Person.id == personId).first()
                if person is None:
                    raise Exception(f"Отстутствует сотрудник с id= {personId}")
                #Найти тип прохода через проходную
                pass_type_name = pass_type_name.upper()
                pass_type = session.query(PassType).filter(PassType.typeName == pass_type_name).first()
                if pass_type is None:
                    raise Exception(f"Отстутствует тип прохода в Бд  {pass_type_name}")
                #Найти проходную по имени
                gate_name = gate_name.upper()
                gate = session.query(Gate).filter(Gate.name == gate_name).first()
                if gate is None:
                    raise Exception(f"Отстутствует в БД проходная  с именем  {gate_name}")
                date_time = datetime.strptime(date, '%Y-%m-%d %H:%M')
                gate_pass_time = GatePassTime(timestamp=date_time, pick_type_id=pass_type.id, person_id=person.id, gate_id=gate.id, isHoliday=date_time.date() in holidays)
                session.add(gate_pass_time)
            except Exception as e:
                print(f"Не удалось добавить записи в таблицу Отметка о прохождении через проходную. {department_name} {personId} {date} {pass_type_name} {gate_name}. {e}")
        session.commit()
    except Exception as e:
        print(f"Не удалось добавить записи в таблицу отметки прхождения проходной. {e}")
    finally:
        session.close()

def add_types_gates(get_types_list: list[str]):
    session = SessionLocal()
    try:
        for name in get_types_list:
            name = name.upper()
            exsist_gate_type = session.query(GateType).filter(GateType.gateTypeName == name).first()
            if exsist_gate_type is None:
                gate = GateType(gateTypeName=name)
                session.add(gate)
        session.commit()
    except Exception as e:
        print(f"Не удалось добавить записи в таблицу Проходные. {e}")
    finally:
        session.close()

def add_pass_types(pick_types_name: list[str]):
    session = SessionLocal()
    try:
        for name in pick_types_name:
            name = name.upper()
            exsist_gate_type = session.query(PassType).filter(PassType.typeName == name).first()
            if exsist_gate_type is None:
                pass_type = PassType(typeName=name)
                session.add(pass_type)
        session.commit()
    except Exception as e:
        print(f"Не удалось добавить записи в таблицу тип отметки. {e}")
    finally:
        session.close()

def add_person_list(person_list_id: list[int]):
    session = SessionLocal()
    try:
        for id in person_list_id:
            exsist_person = session.query(Person).filter(Person.id == id).first()
            if exsist_person is None:
                person = Person(id=id)
                session.add(person)
        session.commit()
    except Exception as e:
        print(f"Не удалось добавить записи в таблицу Сотрудники. {e}")
    finally:
        session.close()
