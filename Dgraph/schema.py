"""
Statements de Dgraph
"""

SCHEMA = """
type Jugador {  
  id: ID!  
  nombre: String! @search(by: [term, fulltext, trigram])  
  apellido: String! @search(by: [term, fulltext, trigram])  
  numero: Int  
  fechaNacimiento: DateTime @search(by: [datetime]) 
  posicion: String @search(by: [exact])  
  nacionalidad: String  
  altura: Float  
  peso: Float  
  juega_para: Equipo @hasInverse(field: jugadores) 
  tiene_stats: [EstadisticasJugador] @hasInverse(field: de_jugador) 
  stats_compartidas1: [EstadisticasCompartidas] @hasInverse(field: jugador1) 
  stats_compartidas2: [EstadisticasCompartidas] @hasInverse(field: jugador2) 
} 
type Equipo {  
  id: ID!  
  nombre: String! @search(by: [term, exact, trigram])  
  deporte: String! @search(by: [exact])  
  liga: String @search(by: [exact])  
  fundacion: DateTime  
  ciudad: String  
  usa: Estrategia @hasInverse(field: usado_por) 
  participa_en: [TemporadaEquipo] @hasInverse(field: equipo) 
  jugadores: [Jugador] @hasInverse(field: juega_para) 
  enfrentamientos_local: [Enfrentamiento] @hasInverse(field: es_local) 
  enfrentamientos_visitante: [Enfrentamiento] @hasInverse(field: es_visitante) 
  rivalidades1: [Rivalidad] @hasInverse(field: rival1) 
  rivalidades2: [Rivalidad] @hasInverse(field: rival2) 
  desempenos_campo: [DesempenoCampo] @hasInverse(field: equipo) 
  stats_compartidas_equipo: [EstadisticasCompartidas] @hasInverse(field: en_equipo) 
} 
type CampoJuego {  
  id: ID!  
  nombre: String! @search(by: [term, trigram])  
  ubicacion: String  
  capacidad: Int  
  tipo: String 
  superficie: String
  partidos: [Enfrentamiento] @hasInverse(field: jugado_en) 
  desempenos_equipo: [DesempenoCampo] @hasInverse(field: campo) 
} 
type Temporada {  
  id: ID!  
  anio: Int @search(by: [int])  
  nombre: String  
  liga: String @search(by: [exact])  
  deporte: String @search(by: [exact])  
  fechaInicio: DateTime @search(by: [datetime]) 
  fechaFin: DateTime @search(by: [datetime]) 
  equipos_participantes: [TemporadaEquipo] @hasInverse(field: temporada) 
  partidos: [Enfrentamiento] @hasInverse(field: temporada) 
  estadisticas_jugadores: [EstadisticasJugador] @hasInverse(field: temporada) 
  estadisticas_compartidas: [EstadisticasCompartidas] @hasInverse(field: temporada) 
} 
type TemporadaEquipo {  
  id: ID!  
  equipo: Equipo! @hasInverse(field: participa_en) 
  temporada: Temporada! @hasInverse(field: equipos_participantes) 
  posicionFinal: Int @search(by: [int])  
  victorias: Int  
  derrotas: Int  
  empates: Int  
  puntosTotal: Int @search(by: [int])  
  puntosAFavor: Int  
  puntosEnContra: Int  
} 
type Estrategia {  
  id: ID!  
  nombre: String! @search(by: [term, trigram])  
  formacion: String  
  estilo: String @search(by: [exact])  
  descripcion: String  
  deporte: String @search(by: [exact])  
  usado_por: [Equipo] @hasInverse(field: usa) 
} 
type EstadisticasJugador {  
  id: ID!  
  de_jugador: Jugador! @hasInverse(field: tiene_stats) 
  equipo: Equipo! 
  temporada: Temporada! @hasInverse(field: estadisticas_jugadores) 
  partidosJugados: Int  
  puntosTotal: Int  
  asistenciasTotal: Int  
  amonestaciones: Int  
  expulsiones: Int  
  minutosJugados: Int  
} 
type Enfrentamiento {  
  id: ID!  
  es_local: Equipo! @hasInverse(field: enfrentamientos_local) 
  es_visitante: Equipo! @hasInverse(field: enfrentamientos_visitante) 
  jugado_en: CampoJuego @hasInverse(field: partidos) 
  fecha: DateTime @search(by: [datetime])  
  resultado: String @search(by: [exact])  
  puntosLocal: Int  
  puntosVisitante: Int  
  temporada: Temporada! @hasInverse(field: partidos) 
} 
type EstadisticasCompartidas {  
  id: ID!  
  jugador1: Jugador! @hasInverse(field: stats_compartidas1) 
  jugador2: Jugador! @hasInverse(field: stats_compartidas2) 
  en_equipo: Equipo! @hasInverse(field: stats_compartidas_equipo) 
  partidosJuntos: Int  
  victoriasJuntos: Int  
  amonestacionesJuntos: Int  
  pasesJuntos: Int  
  temporada: Temporada! @hasInverse(field: estadisticas_compartidas) 
} 
type Rivalidad {  
  id: ID!  
  rival1: Equipo! @hasInverse(field: rivalidades1) 
  rival2: Equipo! @hasInverse(field: rivalidades2) 
  enfrentamientosTotales: Int  
  victoriasEquipo1: Int  
  victoriasEquipo2: Int  
  empates: Int  
  ultimoEnfrentamiento: DateTime @search(by: [datetime]) 
} 
type DesempenoCampo {  
  id: ID!  
  equipo: Equipo! @hasInverse(field: desempenos_campo) 
  campo: CampoJuego! @hasInverse(field: desempenos_equipo) 
  partidosJugados: Int  
  victorias: Int  
  derrotas: Int  
  empates: Int  
  rendimientoPct: Float  
  puntosAnotados: Int  
  puntosRecibidos: Int  
} 
"""