package br.com.alura.ecommerce.dispatcher;

import br.com.alura.ecommerce.CorrelationId;
import br.com.alura.ecommerce.Message;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Dispatche é o produtor. Ele só não chamou de KafkaProducer pois já existe
 * essa classe.
 * 
 * Estende Closeable para que feche a porta caso ocorra qualquer problema. É
 * implementado o método close.
 */
public class KafkaDispatcher<T> implements Closeable {

	private final KafkaProducer<String, Message<T>> producer;

	public KafkaDispatcher() {
		// gera o podutor
		this.producer = new KafkaProducer<>(properties());
	}

	private static Properties properties() {
		var properties = new Properties();
		// IP do Kafka
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		// serializador = transformador de String para bytes
		// aqui passo o serializer (transformador) da key
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		// aqui passo o serializer (transformador) do value (que eh a mensagem)
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());

		// exige que o OK serja dando somente quando todas as réplicas confirmarem que
		// receberam cópia da mensagem do líder.
		// se clicar com Ctrl + click em cima do ACKS_CONFIG ele leva para a documentação
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

		// a quantidade de tempo que as msgs ficam armazenadas no servidor e a
		// quantidade maxima de memoria que elas ocupam sao
		// propriedades definidas no server
		return properties;
	}

	public void send(String topic, String key, CorrelationId id, T payload)
			throws ExecutionException, InterruptedException {
		Future<RecordMetadata> future = sendAsync(topic, key, id, payload);
		future.get();
	}

	/**
	 * Produzir mensagens se resume a, criar um produtor, criar a mensagem (record),
	 * enviar, e colocar um listener (callback) que fica escutando se deu sucesso na
	 * mensagem ou erro
	 * 
	 * @param topic   - eh o topico para o qual a mensagem será enviada
	 * @param key
	 * @param id
	 * @param payload
	 * @return Future eh algo que será executado daqui a pouco. Ele eh assincrono.
	 *         Não é bloqueante.
	 */
	public Future<RecordMetadata> sendAsync(String topic, String key, CorrelationId id, T payload) {
		var value = new Message<>(id.continueWith("_" + topic), payload);
		// a mensagem para o kafka eh chamada de record, pois vai ficar registrada no
		// kafka
		// a chave definie para qual partição enviará a mensagem. Na primeira fez define
		// a partição para a key, depois sempre enviará essa mesma key para a mesma
		// partição.
		// O Kafka usa um algoritmo de Hash para definir para qual partição vai a key
		var record = new ProducerRecord<>(topic, key, value);

		// O callback será informado quando a mensagem for processada assincronamente.
		// O construtor recebe os metadados de sucesso (data), ou a exception de falha
		// (ex)
		Callback callback = (data, ex) -> {
			if (ex != null) { // significa que deu erro
				ex.printStackTrace();
				return;
			}
			// pode-se dizer que offset é a posição do registro no broker
			// ex: mensagem posição 0, posição 1, etc, tipo array
			System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset "
					+ data.offset() + "/ timestamp " + data.timestamp());
		};
		return producer.send(record, callback);
	}

	@Override
	public void close() {
		producer.close();
	}
}
