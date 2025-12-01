import csv
from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"
DB_NOMBRE = "sports_db"

ARCHIVO_JUGADORES = "./data/jugadores.csv"
ARCHIVO_EQUIPOS = "./data/equipos.csv"
ARCHIVO_ESTADISTICAS = "./data/estadisticas_torneos.csv"
ARCHIVO_PARTIDOS = "./data/partidos.csv"
ARCHIVO_LIGAS = "./data/ligas.csv"

COLLECTION_JUGADORES = "jugadores"
COLLECTION_EQUIPOS = "equipos"
COLLECTION_ESTADISTICAS = "estadisticas_torneos"
COLLECTION_PARTIDOS = "partidos"
COLLECTION_LIGAS = "ligas"

def poblar_equipos_db():
    client = MongoClient(MONGO_URI)
    db = client[DB_NOMBRE]
    col = db[COLLECTION_EQUIPOS]

    docs = []
    mapa_ids = {}

    with open(ARCHIVO_EQUIPOS, "r", encoding="utf-8") as fd:
        reader = csv.DictReader(fd)
        for fila in reader:
            equipo = {
                "nombre": fila["nombre"],
                "deporte": fila["deporte"],
                "pais": fila["pais"],
                "region": fila["region"],
                "estadisticas_acumuladas": {
                    "trofeos_totales": int(fila["trofeos_totales"]),
                    "puntos_historicos": int(fila["puntos_historicos"])
                },
                "isActive": fila["activo"].strip().lower() in ("si", "sí", "true", "1")
            }
            docs.append(equipo)

    resultado = col.insert_many(docs)

    with open(ARCHIVO_EQUIPOS, "r", encoding="utf-8") as fd:
        reader = csv.DictReader(fd)
        for fila, obj_id in zip(reader, resultado.inserted_ids):
            mapa_ids[fila["jugadores_ids"].strip()] = obj_id

    client.close()
    return mapa_ids


def poblar_jugadores_db(mapa_ids):
    client = MongoClient(MONGO_URI)
    db = client[DB_NOMBRE]
    col = db[COLLECTION_JUGADORES]

    docs = []

    with open(ARCHIVO_JUGADORES, "r", encoding="utf-8") as fd:
        reader = csv.DictReader(fd)
        for fila in reader:
            equipo_oid = mapa_ids.get(fila["equipo_id"].strip())

            jugador = {
                "nombre": fila["nombre"],
                "apellido": fila["apellido"],
                "numero": int(fila["numero"]),
                "fecha_nacimiento": fila["fecha_nacimiento"],
                "deporte": fila["deporte"],
                "pais_origen": fila["pais_origen"],
                "equipo_id": equipo_oid,
                "atributos_adicionales": {
                    "posicion": fila["posicion"],
                    "altura_cm": int(fila["altura_cm"])
                }
            }

            docs.append(jugador)

    resultado = col.insert_many(docs)
    client.close()
    return docs, resultado.inserted_ids


def actualizar_equipos(jugadores_docs, jugadores_oids):
    client = MongoClient(MONGO_URI)
    db = client[DB_NOMBRE]
    col = db[COLLECTION_EQUIPOS]

    agrupados = {}

    for jugador, oid in zip(jugadores_docs, jugadores_oids):
        equipo_id = jugador["equipo_id"]
        if equipo_id:
            agrupados.setdefault(equipo_id, []).append(oid)

    for equipo_oid, lista_jugadores in agrupados.items():
        col.update_one(
            {"_id": equipo_oid},
            {"$set": {"jugadores_ids": lista_jugadores}}
        )

    client.close()


def poblar_estadisticas_equipos(mapa_ids):
    client = MongoClient(MONGO_URI)
    db = client[DB_NOMBRE]
    col = db[COLLECTION_ESTADISTICAS]

    docs = []

    with open(ARCHIVO_ESTADISTICAS, "r", encoding="utf-8") as fd:
        reader = csv.DictReader(fd)
        for fila in reader:
            csv_id = fila["equipo_csv_id"].strip()
            if csv_id not in mapa_ids:
                continue

            doc = {
                "equipo_id": mapa_ids[csv_id],
                "deporte": fila["deporte"],
                "torneo_nombre": fila["torneo_nombre"],
                "temporada": fila["temporada"],
                "metricas": {
                    "puntos": int(fila["puntos"]),
                    "victorias": int(fila["victorias"]),
                    "derrotas": int(fila["derrotas"]),
                    "goles_a_favor": int(fila["goles_a_favor"])
                }
            }

            docs.append(doc)

    if docs:
        col.insert_many(docs)

    client.close()


def poblar_partidos_db(mapa_ids):
    client = MongoClient(MONGO_URI)
    db = client[DB_NOMBRE]
    col = db[COLLECTION_PARTIDOS]

    docs = []
    referencias = []

    with open(ARCHIVO_PARTIDOS, "r", encoding="utf-8") as fd:
        reader = csv.DictReader(fd)
        for fila in reader:
            partido = {
                "deporte": fila["deporte"],
                "fecha": datetime.strptime(fila["fecha"], "%Y-%m-%d"),
                "torneo_nombre": fila["torneo_nombre"],
                "equipo_local_id": None,
                "equipo_visitante_id": None,
                "marcador": {
                    "local": int(fila["goles_local"]),
                    "visitante": int(fila["goles_visitante"])
                }
            }

            docs.append(partido)
            referencias.append((
                fila["equipo_local_csv_id"].strip(),
                fila["equipo_visitante_csv_id"].strip()
            ))

    resultado = col.insert_many(docs)

    for (local_csv, visitante_csv), obj_id in zip(referencias, resultado.inserted_ids):
        col.update_one(
            {"_id": obj_id},
            {"$set": {
                "equipo_local_id": mapa_ids.get(local_csv),
                "equipo_visitante_id": mapa_ids.get(visitante_csv)
            }}
        )

    client.close()


def poblar_ligas_db():
    client = MongoClient(MONGO_URI)
    db = client[DB_NOMBRE]
    col = db[COLLECTION_LIGAS]

    docs = []

    with open(ARCHIVO_LIGAS, "r", encoding="utf-8") as fd:
        reader = csv.DictReader(fd)
        for fila in reader:
            item = {
                "nombre_deporte": fila["nombre_deporte"],
                "ligas": [
                    {
                        "nombre_liga": fila["liga_1_nombre"],
                        "fecha_inicio": fila["liga_1_fecha_inicio"],
                        "cupos_equipos": int(fila["liga_1_cupos"])
                    },
                    {
                        "nombre_liga": fila["liga_2_nombre"],
                        "fecha_inicio": fila["liga_2_fecha_inicio"],
                        "cupos_equipos": int(fila["liga_2_cupos"])
                    }
                ]
            }
            docs.append(item)

    col.insert_many(docs)
    client.close()
    return len(docs)


def poblar():
    mapa_ids = poblar_equipos_db()
    jugadores_docs, jugadores_ids = poblar_jugadores_db(mapa_ids)
    actualizar_equipos(jugadores_docs, jugadores_ids)
    poblar_estadisticas_equipos(mapa_ids)
    poblar_partidos_db(mapa_ids)
    poblar_ligas_db()
