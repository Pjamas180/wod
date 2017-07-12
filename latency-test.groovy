@Grapes([
    @Grab('com.github.rahulsom:genealogy:1.0'),
    @Grab('ca.uhn.hapi:hapi-base:2.1'),
    @Grab('ca.uhn.hapi:hapi-structures-v26:2.1')
])
/**
 * 1. Configure your lab to send HL7 to the machine running this script on port 21112
 * 2. Configure your clinic to send HL7 to the machine running this script on port 21110
 * 3. Configure the map called `config`
 *
 * 4. Run this as:
 *
 *   groovy latency-test.groovy
 *
 */

import ca.uhn.hl7v2.DefaultHapiContext
import ca.uhn.hl7v2.HL7Exception
import ca.uhn.hl7v2.HapiContext
import ca.uhn.hl7v2.app.*
import ca.uhn.hl7v2.llp.MinLowerLayerProtocol
import ca.uhn.hl7v2.model.Message
import ca.uhn.hl7v2.model.v26.message.ORM_O01
import ca.uhn.hl7v2.model.v26.segment.MSH
import ca.uhn.hl7v2.parser.CanonicalModelClassFactory
import ca.uhn.hl7v2.parser.GenericParser
import ca.uhn.hl7v2.parser.PipeParser
import ca.uhn.hl7v2.protocol.ReceivingApplicationException
import ca.uhn.hl7v2.validation.builder.support.NoValidationBuilder
import com.github.rahulsom.genealogy.NameDbUsa
import groovy.time.TimeCategory
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

import java.security.MessageDigest
import java.security.SecureRandom
import java.text.SimpleDateFormat
import java.util.concurrent.Executors

/*
 * There are 2 groups of config - clinic and hospital
 * Each has host, port, orders/results, and delay
 * The first 3 are self explanatory. The fourth is the mean SD of a gaussian distribution
 */
def config = [
    'clinic.host'     : 'localhost',
    'clinic.port'     : 8899,
    'clinic.orders'   : 10000,
    'clinic.delay'    : 1000,
    'hospital.host'   : 'localhost',
    'hospital.port'   : 8903,
    'hospital.results': 0,
    'hospital.delay'  : 300,
]

class LatencyTestUtil {

  ConnectionHub connectionHub = ConnectionHub.instance
  PrintWriter csv

  LatencyTestUtil() {
    if (new File('load.csv').exists()) {
      new File('load.csv').delete()
    }
    csv = new File('load.csv').newPrintWriter()
    csv.println("Time,Type,Latency")

  }

  static <T> T time(@ClosureParams(value = SimpleType, options = "java.lang.StringBuilder") Closure<T> closure) {
    def builder = new StringBuilder()
    def start = new Date()
    T retval = closure(builder)
    def end = new Date()
    def duration = use(TimeCategory) {
      end - start
    }
    def indent = '\t' * 4
    System.err.println "${indent}${builder} ${duration}"
    retval
  }

  void logCsv(String type, long millis) {
    csv.println "${new Date().format('yyyy-MM-dd\'T\'HH:mm:ss')},${type},${millis}"
    csv.flush()
  }

  Application createLab(List pendingOrders ,List resultsQueue) {
    new Application() {
      @Override
      Message processMessage(Message message) throws ReceivingApplicationException, HL7Exception {
        MSH msh = message.getMSH()
        def now = new Date()

        def inFormat = 'yyyyMMddHHmmss.SSS'
        def outFormat = 'HH:mm:ss.SSS'

        def time = new SimpleDateFormat(inFormat).parse(msh.dateTimeOfMessage.encode().substring(0, 17))
        def duration = use(TimeCategory) {
          now - time
        }
        logCsv('ORM.Latency', duration.toMilliseconds())

        def orderId = ((ORM_O01)message).ORDER.ORC.placerOrderNumber.encode().substring(38)
        def controlId = msh.messageControlID.value
        def indent = '\t' * 9
        println "${indent}ORM\t${orderId}\t${now.format(outFormat)}\t${time.format(outFormat)}\t${duration}"

        msh.messageType.parse('ORU^R01')
        msh.messageControlID.parse("${orderId}!")
        resultsQueue.add(message.encode())
        System.err.println "Storing Result '${orderId}!'"
        pendingOrders.remove(controlId)
        message.generateACK()
      }

      @Override
      boolean canProcess(Message message) {
        return true
      }
    }
  }

  Application createClinic(List pendingResults) {
    new Application() {
      @Override
      Message processMessage(Message message) throws ReceivingApplicationException, HL7Exception {
        MSH msh = message.getMSH()
        def now = new Date()

        def inFormat = 'yyyyMMddHHmmss.SSS'
        def outFormat = 'HH:mm:ss.SSS'
        def time = new SimpleDateFormat(inFormat).parse(msh.dateTimeOfMessage.encode().substring(0, 17))
        pendingResults.remove(msh.messageControlID.value)
        def duration = use(TimeCategory) {
          now - time
        }
        logCsv('ORU.Latency', duration.toMilliseconds())
        def indent = '\t' * 9
        println "${indent}ORU\t${msh.messageControlID.value}\t${now.format(outFormat)}\t${time.format(outFormat)}\t${duration}"
        message.generateACK()
      }

      @Override
      boolean canProcess(Message message) {
        return true
      }
    }
  }

  def generateOrder(int index, List<String> pendingOrders) {
    def now = new Date()
    def timestamp = now.format('yyyyMMddHHmmss.SSSZ')

    def messageDigest = MessageDigest.getInstance("SHA-256")
    messageDigest.update(now.time.toBigInteger().toByteArray())
    def msgId = new BigInteger(1, messageDigest.digest()).toString(16).padLeft(40, '0')

    NameDbUsa instance = NameDbUsa.instance
    def gender = new SecureRandom().nextBoolean() ? 'M' : 'F'
    String firstName = gender == 'M' ? instance.maleName : instance.femaleName
    String lastName = instance.lastName

    System.err.println "Sending ORM #$index for ${msgId[0..8]}"
    // Uncomment this if you want a simple order

    pendingOrders << msgId[0..8]
    """\
    |MSH|^~\\&|LinkLogic-TEST|TEST000^PMS|LabQuest|PMS|${timestamp}||ORM^O01|${msgId[0..8]}|P|2.6|||NE|NE
    |PID|1||${msgId[0..6]}|PEER-OUT|${lastName}^${firstName}||19470612|M||W|12155 SW Broadway^^Beaverton^OR^97005^USA||^^^dbassett@aol.com^^503^6295541|^^^^^503^6928955|English|M|||543-34-5621
    |PV1|1|O|^^^PMS||||Attend20
    |IN1|1||Futura|Best Health Insurance Company|825 NE Roosevelt^^Portland^OR^97213|Betty Hill|^^^betty_hill@bhi.com^^503^2395800|BHI8654||||19990416||||Bassett^Don^C.|S|19470612|12155 SW Broadway^^Beaverton^OR^97005^USA|||P||||||||||||||543-34-5621||||||F|M
    |ORC|NW|${msgId[0..8]}|CPT-83718^HDL|946281^PC|||1^QAM||20110407|||doogie^^^^^^^^1003&2.16.4.39.2.1001.79&ISO
    |OBR|1|${msgId[0..8]}|||||20130805||1||N|||||Attend20|||||||||||^^^^^R
    """.stripMargin().replace('\n', '\r')


    // Uncomment this if you want recurring order
/*
    6.times {pendingOrders << msgId[0..8]}
    """\
    |MSH|^~\\&|LinkLogic-TEST|TEST000^PMS|LabQuest|PMS|${timestamp}||ORM^O01|${msgId[0..8]}|P|2.6|||NE|NE
    |PID|1||${msgId[0..6]}|PEER-OUT|${lastName}^${firstName}||19470612|M||W|12155 SW Broadway^^Beaverton^OR^97005^USA||^^^dbassett@aol.com^^503^6295541|^^^^^503^6928955|English|M|||543-34-5621
    |PV1|1|O|^^^PMS||||Attend20
    |IN1|1||Futura|Best Health Insurance Company|825 NE Roosevelt^^Portland^OR^97213|Betty Hill|^^^betty_hill@bhi.com^^503^2395800|BHI8654||||19990416||||Bassett^Don^C.|S|19470612|12155 SW Broadway^^Beaverton^OR^97005^USA|||P||||||||||||||543-34-5621||||||F|M
    |ORC|NW|${msgId[0..8]}-1|CPT-83718^HDL|946281^PC|||1^QAM||20110407|||doogie^^^^^^^^1003&2.16.4.39.2.1001.79&ISO
    |OBR|1|${msgId[0..8]}-1|||||20130805||1||N|||||Attend20|||||||||||^^^^^R
    |ORC|NW|${msgId[0..8]}-2|CPT-82465^Cholesterol|946281^PC|||5^QAM||20110407|||hwinston^Test^Fun
    |OBR|1|${msgId[0..8]}-2||CPT-82465^Cholesterol|||20110407||1||N|||||hwinston^Test^Fun|||||||||||^^^^^R
    |OBX|1|TX|FO^Future Order||Y
    """.stripMargin().replace('\n', '\r')
*/

    // Uncomment this if you want complex order
//    6.times {pendingOrders << msgId[0..8]}
//    """\
//    |MSH|^~\\&|LinkLogic-TEST|TEST000^PMS|LabQuest|PMS|${timestamp}||ORM^O01|${msgId[0..8]}|P|2.6|||NE|NE
//    |PID|1||${msgId[0..6]}|PEER-OUT|${lastName}^${firstName}||19470612|M||W|12155 SW Broadway^^Beaverton^OR^97005^USA||^^^dbassett@aol.com^^503^6295541|^^^^^503^6928955|English|M|||543-34-5621
//    |PV1|1|O|^^^PMS||||Attend20
//    |IN1|1||Futura|Best Health Insurance Company|825 NE Roosevelt^^Portland^OR^97213|Betty Hill|^^^betty_hill@bhi.com^^503^2395800|BHI8654||||19990416||||Bassett^Don^C.|S|19470612|12155 SW Broadway^^Beaverton^OR^97005^USA|||P||||||||||||||543-34-5621||||||F|M
//    |ORC|NW|${msgId[0..8]}-1|CPT-83718^HDL|946281^PC|||1^QAM||20110407|||doogie^^^^^^^^1003&2.16.4.39.2.1001.79&ISO
//    |OBR|1|${msgId[0..8]}-1|||||20130805||1||N|||||Attend20|||||||||||^^^^^R
//    |ORC|NW|${msgId[0..8]}-2|CPT-82465^Cholesterol|946281^PC|||1^QAM||20110407|||hwinston^Test^Fun
//    |OBR|1|${msgId[0..8]}-2||CPT-82465^Cholesterol|||20110407||1||N|||||hwinston^Test^Fun|||||||||||^^^^^R
//    |ORC|NW|${msgId[0..8]}-3|CPT-82466^Glucose|946281^PC|||1^QAM||20110407|||hwinston^Test^Fun
//    |OBR|1|${msgId[0..8]}-3||CPT-82466^Glucose|||20110407||1||N|||||hwinston^Test^Fun|||||||||||^^^^^R
//    |ORC|NW|${msgId[0..8]}-4|CPT-82467^LDL|946281^PC|||1^QAM||20110407|||hwinston^Test^Fun
//    |OBR|1|${msgId[0..8]}-4||CPT-82467^LDL|||20110407||1||N|||||hwinston^Test^Fun|||||||||||^^^^^R
//    |ORC|NW|${msgId[0..8]}-5|CPT-82468^D3|946281^PC|||1^QAM||20110407|||hwinston^Test^Fun
//    |OBR|1|${msgId[0..8]}-5||CPT-82468^D3|||20110407||1||N|||||hwinston^Test^Fun|||||||||||^^^^^R
//    |ORC|NW|${msgId[0..8]}-6|CPT-82469^B12|946281^PC|||1^QAM||20110407|||hwinston^Test^Fun
//    |OBR|1|${msgId[0..8]}-6||CPT-82469^B12|||20110407||1||N|||||hwinston^Test^Fun|||||||||||^^^^^R
//    """.stripMargin().replace('\n', '\r')
  }

  def generateUnsolicitedResult(int index) {
    def now = new Date()
    def timestamp = now.format('yyyyMMddHHmmss.SSSZ')

    def messageDigest = MessageDigest.getInstance("SHA-256")
    messageDigest.update(now.time.toBigInteger().toByteArray())
    def msgId = new BigInteger(1, messageDigest.digest()).toString(16).padLeft(40, '0')

    NameDbUsa instance = NameDbUsa.instance
    def gender = new SecureRandom().nextBoolean() ? 'M' : 'F'
    String firstName = gender == 'M' ? instance.maleName : instance.femaleName
    String lastName = instance.lastName

    System.err.println "Sending ORU #$index for ${msgId[0..8]}"
    """\
    |MSH|^~\\&|LinkLogic-TEST|TEST000^PMS|LabQuest|PMS|${timestamp}||ORU^R01|${msgId[0..8]}|P|2.6|||NE|NE
    |PID|1||${msgId[0..6]}|PEER-OUT|${lastName}^${firstName}||19470612|M||W|12155 SW Broadway^^Beaverton^OR^97005^USA||^^^dbassett@aol.com^^503^6295541|^^^^^503^6928955|English|M|||543-34-5621
    |PV1|1|O|^^^PMS||||Attend20
    |IN1|1||Futura|Best Health Insurance Company|825 NE Roosevelt^^Portland^OR^97213|Betty Hill|^^^betty_hill@bhi.com^^503^2395800|BHI8654||||19990416||||Bassett^Don^C.|S|19470612|12155 SW Broadway^^Beaverton^OR^97005^USA|||P||||||||||||||543-34-5621||||||F|M
    |ORC|NW|${msgId[0..8]}|CPT-83718^HDL|946281^PC|||1^QAM||20110407|||D13394^^^^^^^^204&2.16.4.39.2.1003.81&ISO
    |OBR|1|${msgId[0..8]}|||||20130805||1||N|||||Attend20|||||||||||^^^^^R
    """.stripMargin().replace('\n', '\r')
  }
}

def ltu = new LatencyTestUtil()
def pendingOrders = []
def resultsQueue = []
def pendingResults = []
HapiContext ctx = new DefaultHapiContext()
ctx.setValidationRuleBuilder(new NoValidationBuilder())

HL7Service labServer = ctx.newServer(21112, false)
labServer.registerApplication('ORM', 'O01', ltu.createLab(pendingOrders, resultsQueue))
labServer.startAndWait()

HL7Service clinicServer = ctx.newServer(21110, false)
clinicServer.registerApplication('ORU', 'R01', ltu.createClinic(pendingResults))
clinicServer.startAndWait()

def random = new SecureRandom()
def es = Executors.newFixedThreadPool(3)
def ordComplete = es.submit {
  Connection clinicConnection = ltu.connectionHub.attach(config['clinic.host'], config['clinic.port'], new PipeParser(), MinLowerLayerProtocol)
  Initiator clinicInitiator = clinicConnection.initiator
  clinicInitiator.setTimeoutMillis(30000)
  config['clinic.orders'].times { ctr ->
    def p = new GenericParser(new CanonicalModelClassFactory('2.6'))
    def input = ltu.generateOrder(ctr + 1, pendingOrders)
    def m = p.parse(input)
    ltu.time { builder ->
      builder.append("${m.MSH.messageControlID.value} responded in ")
      try { clinicInitiator.sendAndReceive(m) } catch (ignored) { builder.append("error ")}
    }
    def nextMessageIn = Math.abs(random.nextGaussian() * config['clinic.delay']) as long
    sleep(nextMessageIn)
  }
}

Connection hospitalConnection = ltu.connectionHub.attach(config['hospital.host'], config['hospital.port'], new PipeParser(), MinLowerLayerProtocol)
Initiator hopsitalInitiator = hospitalConnection.initiator
hopsitalInitiator.setTimeoutMillis(30000)
def resComplete = es.submit {
  config['hospital.results'].times { ctr ->
    def p = new GenericParser(new CanonicalModelClassFactory('2.6'))
    def input = ltu.generateUnsolicitedResult(ctr + 1)
    def m = p.parse(input)
    pendingResults << m.MSH.messageControlID.value
    ltu.time { builder ->
      builder.append("${m.MSH.messageControlID.value} responded in ")
      try { hopsitalInitiator.sendAndReceive(m) } catch (ignored) {builder.append("error ") }
    }
    def nextMessageIn = Math.abs(random.nextGaussian() * config['hospital.delay']) as long
    sleep(nextMessageIn)
  }
}

sleep 10000
while (pendingOrders || resultsQueue || !ordComplete.done || !resComplete.done) {
  if (resultsQueue.size() < 10) {
    sleep(Math.abs(random.nextGaussian() * 1000) as long)
  }
  System.err.println "Queued Results: ${resultsQueue.size()}\tPending Orders: ${pendingOrders.size()}\tPending Results: ${pendingResults.size()}"
  if (resultsQueue) {
    l = resultsQueue.size()
    def result = resultsQueue[new SecureRandom().nextInt(l)]
    resultsQueue.remove(result)
    def p = new GenericParser(new CanonicalModelClassFactory('2.6'))
    def input = result
    if (input) {
      def m = p.parse(input)
      def timestamp = new Date().format('yyyyMMddHHmmss.SSSZ')
      m.MSH.dateTimeOfMessage.parse(timestamp)
      System.err.println "Sending ORU! for ${m.MSH.messageControlID.value}"
      pendingResults << m.MSH.messageControlID.value
      ltu.time { builder ->
        builder.append("${m.MSH.messageControlID.value} responded in ")
        try { hopsitalInitiator.sendAndReceive(m) } catch (ignored) { builder.append("error ") }
      }
    }
  }
}

def ttl = 120
while (ttl -- && pendingResults) {
  sleep (1000)
}
println "Queued Results  : ${resultsQueue}"
println "Pending Results : ${pendingResults}"
println "Pending Orders  : ${pendingOrders}"
System.exit(0)
