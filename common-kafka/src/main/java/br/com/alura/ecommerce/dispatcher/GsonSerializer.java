package br.com.alura.ecommerce.dispatcher;

import br.com.alura.ecommerce.Message;
import br.com.alura.ecommerce.MessageAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

//implementa interface do kafka para serialização
public class GsonSerializer<T> implements Serializer<T> {

    private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create();

    /**
     * serializador genérico para JSON
     * recebe objeto e devolve um array de bytes
     */
    @Override
    public byte[] serialize(String s, T object) {
        return gson.toJson(object).getBytes();
    }
}
