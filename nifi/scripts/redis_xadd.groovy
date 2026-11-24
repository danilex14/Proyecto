// ExecuteGroovyScript (NiFi 1.28): rama A del flujo -> XADD a Redis Stream `sismos:in`.
//
// NiFi 1.28 no trae procesador para Redis Streams (el NAR nifi-redis solo tiene servicios de
// caché/mapa). En vez de añadir dependencias, este script habla el protocolo RESP de Redis
// directamente por socket TCP (solo JDK): toma hasta 500 FlowFiles por ejecución, envía todos los
// XADD en un solo "pipeline" y lee las respuestas. Si algo falla lanza la excepción y NiFi hace
// rollback (los FlowFiles vuelven a la cola y se reintentan: no se pierden eventos).
//
// El formato del mensaje es el mismo que usa simulator.py --sink redis (todo como string, null -> ""),
// así stream_processor.py no distingue si el evento entró por NiFi o directo.
import groovy.json.JsonSlurper
import java.nio.charset.StandardCharsets

def flowFiles = session.get(500)
if (!flowFiles) return

final String HOST = System.getenv('REDIS_HOST') ?: 'redis'
final int PORT = (System.getenv('REDIS_PORT') ?: '6379') as int
final String STREAM = 'sismos:in'
final String MAXLEN = '500000'

// Comando RESP: *<n>\r\n luego $<bytes>\r\n<arg>\r\n por cada argumento
def encode = { List<String> args ->
    def sb = new StringBuilder('*').append(args.size()).append('\r\n')
    args.each { String a ->
        sb.append('$').append(a.getBytes(StandardCharsets.UTF_8).length).append('\r\n').append(a).append('\r\n')
    }
    sb.toString().getBytes(StandardCharsets.UTF_8)
}

def slurper = new JsonSlurper()
def commands = []
flowFiles.each { ff ->
    String text = session.read(ff).withCloseable { it.getText('UTF-8') }
    text.readLines().findAll { it.trim() }.each { line ->
        def parsed = slurper.parseText(line)
        (parsed instanceof List ? parsed : [parsed]).each { Map ev ->
            def args = ['XADD', STREAM, 'MAXLEN', '~', MAXLEN, '*']
            ev.each { k, v -> args << k.toString() << (v == null ? '' : v.toString()) }
            commands << encode(args)
        }
    }
}

def readLine = { InputStream ins ->
    def buf = new ByteArrayOutputStream()
    int c
    while ((c = ins.read()) != -1) {
        if (c == 13) { ins.read(); break }   // \r\n
        buf.write(c)
    }
    if (c == -1) throw new IOException('Redis cerró la conexión')
    new String(buf.toByteArray(), StandardCharsets.UTF_8)
}

new Socket().withCloseable { sock ->
    sock.connect(new InetSocketAddress(HOST, PORT), 2000)
    sock.soTimeout = 5000
    def out = new BufferedOutputStream(sock.outputStream)
    def ins = new BufferedInputStream(sock.inputStream)
    commands.each { out.write(it) }
    out.flush()
    int errors = 0
    commands.size().times {
        String r = readLine(ins)
        if (r.startsWith('$')) { if (r != '$-1') readLine(ins) }   // id del mensaje (bulk string)
        else if (r.startsWith('-')) { errors++; log.error("Redis respondió: ${r}") }
    }
    if (errors) throw new IOException("${errors} XADD fallaron")
}

flowFiles.each { session.transfer(it, REL_SUCCESS) }
