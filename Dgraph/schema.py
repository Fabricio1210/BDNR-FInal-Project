"""
Statements de Dgraph - Schema simplificado según especificación
"""

SCHEMA = """
nombre: string @index(term) .
apellido: string @index(term) .
numero: int .
fechaNacimiento: datetime .
pais: string @index(term) .
juega_para: [uid] .

liga: string @index(exact) .
fundacion: datetime .
ciudad: string .
jugadores: [uid] @reverse .
campo_local: uid .
rivalidad: [uid] .

capacidad: int .
tipo: string .
equipos_locales: [uid] @reverse .
enfrentamientos: [uid] @reverse .

anio: int @index(int) .
fechaInicio: datetime .
fechaFin: datetime .

equipo_local: uid .
equipo_visitante: uid .
campo: uid @reverse .
temporada: uid @reverse .
fecha: datetime @index(year) .
marcadorLocal: int .
marcadorVisitante: int .
resultado: string .
asistencia: int .

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
  asistencia
}
"""