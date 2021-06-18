package br.com.alura.ecommerce.consumer;

import br.com.alura.ecommerce.Message;
import br.com.alura.ecommerce.dispatcher.GsonSerializer;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {
	private final KafkaConsumer<String, Message<T>> consumer;
	private final ConsumerFunction parse;

	/**
	 * 
	 * @param groupId
	 * @param topic      - indica de qual topico vai ficar escutando
	 * @param parse
	 * @param properties
	 */
	public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Map<String, String> properties) {
		this(parse, groupId, properties);

		// indica qual topico vai ficar escutando. Usou singletonList pois é uma maneira
		// rápida de criar lista
		// o normal é cada consumidor ficar escutando de apenas 1 topico, se nao vira
		// bagunça
		consumer.subscribe(Collections.singletonList(topic));
	}

	public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Map<String, String> properties) {
		this(parse, groupId, properties);
		consumer.subscribe(topic);
	}

	/**
	 * cria o consumidor
	 */
	private KafkaService(ConsumerFunction<T> parse, String groupId, Map<String, String> properties) {
		this.parse = parse;
		this.consumer = new KafkaConsumer<>(getProperties(groupId, properties));
	}

	public void run() throws ExecutionException, InterruptedException {
		try (var deadLetter = new KafkaDispatcher<>()) {
			// para ficar sempre ouvindo sem interromper, coloca essa chamada do poll num
			// laço infinito. Se quiser pode ter um sinalizador de quando o serviço deve ser
			// destruído
			while (true) {
				// poll = pergunta se tem mensagem ali dentro durante 100milisegundos
				var records = consumer.poll(Duration.ofMillis(100));

				// se encontrou registros
				if (!records.isEmpty()) {
					System.out.println("Encontrei " + records.count() + " registros");
					for (var record : records) {
						try {
							parse.consume(record);
						} catch (Exception e) {
							e.printStackTrace();
							var message = record.value();
							deadLetter.send("ECOMMERCE_DEADLETTER", message.getId().toString(),
									message.getId().continueWith("DeadLetter"),
									new GsonSerializer().serialize("", message));
						}
					}
				}
			}
		}

	}

	/**
	 * 
	 * @param groupId            -
	 * @param overrideProperties
	 * @return
	 */
	private Properties getProperties(String groupId, Map<String, String> overrideProperties) {
		var properties = new Properties();
		// onde ele vai escutar
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

		// deserializador = classe que vai transformar de bytes para a classe da chave e
		// value
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());

		// é o grupo que o consumidor vai ficar escutando.
		// Se tiver mais de um serviço escutando o mesmo grupo, as mensagens serão
		// distribuídas entre eles.
		// nas propriedades do servidor, também tem a propriedade num.partitions. Cada
		// partição serve como uma sequência de mensagens para cada tópico. Então se
		// tenho apenas 1 partição, haverá somente uma sequência para cada tópico. E
		// assim por diante.
		// Quando levantamos um consumidor, ele se responsabiliza por várias partes
		// (partitions).
		// Não adianta ter mais de um consumidor por partition, pois um deles ficaria
		// parado esperando.
		// OBS: o número máximo de paralelizações, é o número de partições
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

		// temos a possibilidade de dar um ID para o consumidor para facilitar na
		// identificação. Mas é importante que os consumidores tenham IDs diferentes,
		// então não podemos passar um ID estático
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.putAll(overrideProperties);
		return properties;
	}

	@Override
	public void close() {
		consumer.close();
	}
}
