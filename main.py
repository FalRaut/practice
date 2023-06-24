from fastapi import FastAPI, HTTPException
from typing import Dict, List, Optional

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pydantic import BaseModel

import re

CREDENTIALS_FILE = 'fastapi-390301-1f3374e32cef.json'  # Имя файла с закрытым ключом
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
client = gspread.authorize(creds)

sheet = client.open('Первый тестовый документ').sheet1

app = FastAPI()


class Coauthors(BaseModel):
    name: str
    surname: str
    patronymic: Optional[str]


class Node(BaseModel):
    id: str
    telegram_id: Optional[str]
    discord_id: Optional[str]
    email: str
    phone: Optional[str]
    name: str
    surname: str
    patronymic: Optional[str]
    university: str
    student_group: Optional[str]
    title: str
    adviser: str
    coauthors: Optional[List[Coauthors]]


fields = ['id', 'telegram_id', 'discord_id', 'email', 'phone', 'name', 'surname', 'patronymic', 'university',
          'student_group', 'title', 'adviser', 'coauthors']


@app.post("/", response_model=Node)
def post_node(data: Dict):
    # обязательные поля в json
    required_fields = ["name", "surname", "email", "university", "title", "adviser"]

    for field in required_fields:  # перебор по обязательным полям
        if not data.get(field):  # если в словаре на первом уровне вложенности нет записи по обязательному полю
            raise HTTPException(status_code=400, detail=f"Field {field} is empty")  # throw exception

    if data.get('coauthors'):
        for elem in data['coauthors']:  # перебор элементов второго уровня вложенности
            for field in required_fields[:2]:  # перебор по обязательным полям
                if not elem.get(field):  # если в словаре на втором уровне вложенности нет записи по обязательному полю
                    raise HTTPException(status_code=400, detail=f"Field {field} is empty")  # throw exception

    # формирование списка для записи и ответа
    response = [f'{data.get(field, "")}' for field in fields[1:12]]

    # добавление в строку записи и ответа строки с соавторами
    response.append(', '.join(
        [f"{elem['name']} {elem['surname']}{' ' + elem['patronymic'] if elem.get('patronymic') else ''}" for elem in
         data.get('coauthors', [])]))

    # запись данных в таблицу
    desc = sheet.append_row(response)

    # нахождение id (номер добавленной строки)
    match = re.search(r'\d+$', desc.get("updates").get("updatedRange"))

    # удаление ключей со значениями ‘’
    data = {key: value for key, value in data.items() if value not in ['', []]}

    # удаление ключей со значениями ‘’ из соавторов
    for d in data.get('coauthors', []):
        for key in list(d):
            if not d[key]:
                del d[key]

    if match:  # если id был найден (строка успешно добавлена)
        if data.get('id'):  # если id зачем-то был передан
            data['id'] = match.group()  # заменить его правильным id
        else:
            data = {'id': match.group()} | data  # запись id в возвращаемый список

        return data  # возврат словаря с добавленными данными
    else:
        raise HTTPException(status_code=502, detail="Bad Gateway ()")  # throw exception


@app.get("/", response_model=List[Node])
def get(telegram_id: str = None, discord_id: str = None, email: str = None):
    # проверка на кол-во переданных аргументов
    if sum(map(bool, [telegram_id, discord_id, email])) != 1:
        raise HTTPException(status_code=400, detail=f"Only one argument is allowed")  # throw exception

    # список данных таблицы
    rows = sheet.get_all_values()[1:]

    parameter = 0
    requested_value = ''
    for value in [telegram_id, discord_id, email]:
        if value:
            requested_value = value
            break
        parameter += 1

    _id = 2  # индекс строки
    filtered_rows = []  # строки с совпадениями
    for row in rows:  # перебор считанных строк
        if row[parameter] == requested_value:  # если совпадение
            row.insert(0, str(_id))  # добавить в строку id записи

            coauthors = []  # список соавторов для каждой совпавшей строки

            if len(row[-1]):  # если имеются соавторы
                for item in row[-1].split(', '):  # перебор по соавторам
                    items = item.split()  # разделение ФИО соавтора
                    coauthor = {'name': items[0], 'surname': items[1]}
                    if len(items) > 2:  # если соавтор имеет отчество
                        coauthor['patronymic'] = items[2]
                    coauthors.append(coauthor)

            row[-1] = coauthors  # замена поля соавторов словарем соавторов, подходящим для перевода в json

            filtered_rows.append(row)  # сохранить строку
        _id += 1

    # приведение списка к словарю
    dicts = [dict(zip(fields, value)) for value in filtered_rows]

    # удаление ключей со значениями ‘’
    dicts = [{key: value for key, value in d.items() if value not in ['', []]} for d in dicts]

    return dicts
