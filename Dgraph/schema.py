"""
Statements de Dgraph - Schema simplificado según especificación
"""

SCHEMA = """
nombre: string @index(term, exact) .
apellido: string @index(term) .
numero: int .
fechaNacimiento: datetime .
pais: string @index(term) .
juega_para: [uid] @reverse .

liga: string @index(exact) .
fundacion: datetime .
ciudad: string .
jugadores: [uid] @reverse .
campo_local: uid .
rivalidad: [uid] .

capacidad: int .
tipo: string @index(term) .
equipos_locales: [uid] @reverse .
enfrentamientos: [uid] @reverse .

anio: int @index(int) .
fechaInicio: datetime .
fechaFin: datetime .

equipo_local: uid .
equipo_visitante: uid .
campo: uid @reverse .
temporada: uid @reverse .get_team_ranking_by_sport
fecha: datetime @index(year) .
marcadorLocal: int .
marcadorVisitante: int .
resultado: string .

type Jugador {
  nombre
  apellido
  numero
  fechaNacimiento
  pais
  juega_para
}

type Equipo {
  nombre
  liga
  fundacion
  pais
  ciudad
  jugadores
  campo_local
  rivalidad
}

type Campo {
  nombre
  pais
  capacidad
  tipo
  equipos_locales
  enfrentamientos
}

type Temporada {
  anio
  nombre
  liga
  fechaInicio
  fechaFin
  enfrentamientos
}

type Enfrentamiento {
  equipo_local
  equipo_visitante
  campo
  temporada
  fecha
  marcadorLocal
  marcadorVisitante
  resultado
}
"""

# ==================== QUERIES ====================

QUERY_JUGADOR_COMPLETO = """
query jugador_info($nombre: string, $apellido: string) {
    jugador(func: type(Jugador)) @filter(eq(nombre, $nombre) AND eq(apellido, $apellido)) {
        uid
        nombre
        apellido
        numero
        fechaNacimiento
        pais
        juega_para @facets {
            uid
            nombre
            liga
            pais
            ciudad
            campo_local {
                nombre
                pais
                capacidad
                tipo
            }
        }
    }
}
"""

QUERY_ENFRENTAMIENTOS_EQUIPO_TEMPORADA = """
query enfrentamientos($nombre_equipo: string, $nombre_temporada: string) {
    var(func: type(Equipo)) @filter(eq(nombre, $nombre_equipo)) {
        E as uid
    }

    var(func: type(Temporada)) @filter(eq(nombre, $nombre_temporada)) {
        T as uid
    }

    enfrentamientos(func: type(Enfrentamiento)) @filter(uid_in(temporada, uid(T)) AND (uid_in(equipo_local, uid(E)) OR uid_in(equipo_visitante, uid(E)))) {
        uid
        fecha
        marcadorLocal
        marcadorVisitante
        resultado
        equipo_local {
            uid
            nombre
            pais
        }
        equipo_visitante {
            uid
            nombre
            pais
        }
        campo {
            nombre
            pais
            capacidad
            tipo
        }
        temporada {
            nombre
            liga
            anio
        }
    }
}
"""

QUERY_EQUIPOS_LOCALES_ESTADIO = """
query equipos_estadio($nombre_campo: string) {
    campo(func: type(Campo)) @filter(eq(nombre, $nombre_campo)) {
        uid
        nombre
        pais
        capacidad
        tipo
        equipos_locales {
            uid
            nombre
            liga
            pais
            ciudad
            jugadores {
                nombre
                apellido
                numero
                pais
            }
        }
    }
}
"""

QUERY_CAMPOS_EQUIPO = """
query campos_equipo($nombre_equipo: string) {
    equipo(func: type(Equipo)) @filter(eq(nombre, $nombre_equipo)) {
        uid
        nombre
        pais
        campo_local {
            uid
            nombre
            pais
            capacidad
            tipo
        }
    }

    var(func: type(Equipo)) @filter(eq(nombre, $nombre_equipo)) {
        E as uid
    }

    enfrentamientos_visitante(func: type(Enfrentamiento)) @filter(uid_in(equipo_visitante, uid(E))) {
        campo {
            uid
            nombre
            pais
            capacidad
            tipo
        }
    }

    enfrentamientos_local(func: type(Enfrentamiento)) @filter(uid_in(equipo_local, uid(E))) {
        campo {
            uid
            nombre
            pais
            capacidad
            tipo
        }
    }
}
"""

QUERY_ENFRENTAMIENTOS_ESTADIO = """
query enfrentamientos_campo($nombre_campo: string) {
    var(func: type(Campo)) @filter(eq(nombre, $nombre_campo)) {
        C as uid
    }

    enfrentamientos(func: type(Enfrentamiento)) @filter(uid_in(campo, uid(C))) {
        uid
        fecha
        marcadorLocal
        marcadorVisitante
        resultado
        equipo_local {
            uid
            nombre
            liga
            pais
        }
        equipo_visitante {
            uid
            nombre
            liga
            pais
        }
        temporada {
            nombre
            liga
            anio
            fechaInicio
            fechaFin
        }
        campo {
            nombre
            pais
            capacidad
        }
    }
}
"""

QUERY_COMPANEROS_JUGADOR = """
query companeros($nombre: string, $apellido: string) {
    var(func: type(Jugador)) @filter(eq(nombre, $nombre) AND eq(apellido, $apellido)) {
        E as juega_para
    }

    companeros(func: uid(E)) {
        nombre
        liga
        ~juega_para @filter(NOT (eq(nombre, $nombre) AND eq(apellido, $apellido))) {
            nombre
            apellido
            numero
            pais
        }
    }
}
"""

QUERY_RIVALIDADES_EQUIPO = """
query rivalidades($nombre_equipo: string) {
    equipo(func: type(Equipo)) @filter(eq(nombre, $nombre_equipo)) {
        uid
        nombre
        liga
        pais
        rivalidad {
            uid
            nombre
            liga
            pais
        }
    }
}
"""

QUERY_JUGADORES_EQUIPOS_RIVALES = """
query jugadores_rivales($nombre: string, $apellido: string) {
    jugador(func: type(Jugador)) @filter(eq(nombre, $nombre) AND eq(apellido, $apellido)) {
        uid
        nombre
        apellido
        juega_para @facets {
            uid
            nombre
            liga
            rivalidad {
                uid
                nombre
                liga
                pais
            }
        }
    }
}
"""

QUERY_ANTIGUEDAD_JUGADOR = """
query antiguedad($nombre: string, $apellido: string) {
    jugador(func: type(Jugador)) @filter(eq(nombre, $nombre) AND eq(apellido, $apellido)) {
        uid
        nombre
        apellido
        juega_para @facets {
            uid
            nombre
            liga
        }
    }

    var(func: type(Jugador)) @filter(eq(nombre, $nombre) AND eq(apellido, $apellido)) {
        E as juega_para
    }

    temporadas(func: type(Enfrentamiento)) @filter(uid_in(equipo_local, uid(E)) OR uid_in(equipo_visitante, uid(E))) {
        temporada {
            uid
            nombre
            liga
            anio
            fechaInicio
            fechaFin
        }
    }
}
"""

QUERY_IMPACTO_LOCALIA = """
query impacto_localia($nombre_equipo: string) {
    var(func: type(Equipo)) @filter(eq(nombre, $nombre_equipo)) {
        E as uid
        C as campo_local
    }

    equipo(func: uid(E)) {
        uid
        nombre
        liga
        campo_local {
            uid
            nombre
            pais
        }
    }

    local(func: type(Enfrentamiento)) @filter(uid_in(equipo_local, uid(E)) AND uid_in(campo, uid(C))) {
        uid
        fecha
        marcadorLocal
        marcadorVisitante
        resultado
        equipo_visitante {
            nombre
        }
    }

    visitante(func: type(Enfrentamiento)) @filter(uid_in(equipo_local, uid(E)) AND NOT uid_in(campo, uid(C))) {
        uid
        fecha
        marcadorLocal
        marcadorVisitante
        resultado
        equipo_visitante {
            nombre
        }
        campo {
            nombre
            pais
        }
    }
}
"""

QUERY_TEMPORADAS_EQUIPO = """
query temporadas_equipo($nombre_equipo: string) {
    var(func: type(Equipo)) @filter(eq(nombre, $nombre_equipo)) {
        E as uid
    }

    enfrentamientos(func: type(Enfrentamiento)) @filter(uid_in(equipo_local, uid(E)) OR uid_in(equipo_visitante, uid(E))) {
        uid
        fecha
        marcadorLocal
        marcadorVisitante
        resultado
        equipo_local {
            nombre
        }
        equipo_visitante {
            nombre
        }
        temporada {
            uid
            nombre
            liga
            anio
            fechaInicio
            fechaFin
        }
    }
}
""" 