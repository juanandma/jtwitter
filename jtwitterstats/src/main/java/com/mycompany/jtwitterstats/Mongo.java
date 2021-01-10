/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.jtwitterstats;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Indexes.*;
import static com.mongodb.client.model.Projections.*;
import com.sun.org.apache.xml.internal.security.encryption.AgreementMethod;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.swing.JOptionPane;
import org.bson.Document;

/**
 *
 * @author JUANM
 */
public class Mongo {

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> col;

    public MongoDatabase getDatabase() {
        return database;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public MongoCollection<Document> getCol() {
        return col;
    }

    public void initMongoDB() {
        System.out.println("Connecting to Mongo DB..");

        try {
            mongoClient = new MongoClient();
            database = mongoClient.getDatabase("Twitter");
            col = database.getCollection("tweets");
        } catch (MongoException me) {
            System.out.println("Excepción: " + me.getMessage());
        }
    }

    public void shutdownMongoDB() {
        System.out.println("Shutting down connection with Mongo DB.");

        try {
            mongoClient.close();
        } catch (MongoException me) {
            System.out.println("Excepción: " + me.getMessage());
        }
    }
    
    public void palabrasclave(MongoCollection<Document> col) {

        try {
            List<Document> docs = col
                    .aggregate(Arrays.asList(
                            new Document("$group", new Document("_id", "$palabraclave"))
                    )).into(new ArrayList<>());

            //System.out.println("Número de tweets almacenados: " + docs);
            List<String> salida=new ArrayList<>();
            for (Document palabraclave : docs) {
                salida.add(palabraclave.get("_id").toString());
            }
            JOptionPane.showMessageDialog(null, "Palabras claves usadas para la búsqueda: " + salida.toString());

        } catch (MongoException me) {
            //System.out.println("Excepción: " + me.getMessage());
            JOptionPane.showMessageDialog(null, me.getMessage());
        }

    }

    public void tweetsalmacenados(MongoCollection<Document> col) {

        try {
            long docs = col.countDocuments();

            //System.out.println("Número de tweets almacenados: " + docs);
            JOptionPane.showMessageDialog(null, "Número de tweets almacenados: " + docs);

        } catch (MongoException me) {
            //System.out.println("Excepción: " + me.getMessage());
            JOptionPane.showMessageDialog(null, me.getMessage());
        }

    }

    public void fechaprimeryultimo(MongoCollection<Document> col) {

        try {
            List<Document> docs = col.find()
                    .sort(ascending("createdAt"))
                    .into(new ArrayList<>());

            //System.out.println("Fecha del primer tweet: " + docs.get(0).toJson());
            //System.out.println("Fecha del último tweet: " + docs.get(docs.size() - 1).toJson());
            String fecha1 = docs.get(0).get("createdAt").toString();
            String fecha2 = docs.get(docs.size() - 1).get("createdAt").toString();
            JOptionPane.showMessageDialog(null, "Fecha del primer tweet: " + fecha1 + "\nFecha del último tweet: " + fecha2);

        } catch (MongoException me) {
            //System.out.println("Excepción: " + me.getMessage());
            JOptionPane.showMessageDialog(null, me.getMessage());
        }

    }

    public void masretwitteado(MongoCollection<Document> col) {

        try {
            Document docs = col.find()
                    .sort(ascending("retweetCount"))
                    .first();

            //System.out.println("Tweet más retwitteado: " + docs);
            JOptionPane.showMessageDialog(null,  "Tweet más retwitteado: \n" 
                    + " id: "+docs.get("id")+"\n"
                    + " createdAt: "+docs.get("createdAt")+"\n"
                    + " text: "+docs.get("text")+"\n"
                    );
             
        } catch (MongoException me) {
            //System.out.println("Excepción: " + me.getMessage());
            JOptionPane.showMessageDialog(null, me.getMessage());
        }
    }

    public void usuariomastweets(MongoCollection<Document> col) {

        try {
            Document docs = col
                    .aggregate(Arrays.asList(
                            new Document("$group", new Document("_id", "$user.screenName")
                                    .append("tweets", new Document("$sum", 1))),
                            new Document("$sort", new Document("tweets", -1))
                    )).first();

            //System.out.println("Usuario que con más tweets: " + docs.first());
            JOptionPane.showMessageDialog(null, "Usuario que con más tweets: " + docs.get("_id")+"\nTweets: "+docs.get("tweets"));

        } catch (MongoException me) {
            //System.out.println("Excepción: " + me.getMessage());
            JOptionPane.showMessageDialog(null, me.getMessage());
        }
    }

    public void minutocaliente(MongoCollection<Document> col) {

        try {

            Document docs = col
                    .aggregate(Arrays.asList(
                            new Document("$group", new Document("_id", "$createdAt")
                                    .append("tweets", new Document("$sum", 1))),
                            new Document("$sort", new Document("tweets", -1))
                    )).first();

            //System.out.println("Minuto más \"caliente\" (minuto en el que se han escrito más tweets): " + docs.first());
            JOptionPane.showMessageDialog(null, "Minuto más \"caliente\" (minuto en el que se han escrito más tweets): " + docs.get("_id"));

        } catch (MongoException me) {
            //System.out.println("Excepción: " + me.getMessage());
            JOptionPane.showMessageDialog(null, me.getMessage());
        }

    }

    public void urls(MongoCollection<Document> col) {

        try {

            Document docs = col
                    .aggregate(Arrays.asList(
                            new Document("$group", new Document("_id", "$source")
                                    .append("tweets", new Document("$sum", 1))),
                            new Document("$sort", new Document("tweets", -1))
                    )).first();

            //System.out.println("URLs que aparecen más veces en los tweets: " + docs.first());
            JOptionPane.showMessageDialog(null, "URLs que aparecen más veces en los tweets: " + docs.get("_id"));

        } catch (MongoException me) {
            //System.out.println("Excepción: " + me.getMessage());
            JOptionPane.showMessageDialog(null, me.getMessage());
        }

    }

    public void hashtags(MongoCollection<Document> col) {

        try {

            Document docs = col
                    .aggregate(Arrays.asList(
                            new Document("$unwind", "$hashtagEntities"),
                            new Document("$group", new Document("_id", "$hashtagEntities.text")
                                    .append("tweets", new Document("$sum", 1))),
                            new Document("$sort", new Document("tweets", -1))
                    )).first();

            //System.out.println("Hashtags más frecuentes: " + docs.first());
            JOptionPane.showMessageDialog(null, "Hashtags más frecuentes: " + docs.get("_id"));

        } catch (MongoException me) {
            //System.out.println("Excepción: " + me.getMessage());
            JOptionPane.showMessageDialog(null, me.getMessage());
        }

    }

    public void usuariosmasmencionados(MongoCollection<Document> col) {

        try {

            Document docs = col
                    .aggregate(Arrays.asList(
                            new Document("$unwind", "$userMentionEntities"),
                            new Document("$group", new Document("_id", "$userMentionEntities.screenName")
                                    .append("tweets", new Document("$sum", 1))),
                            new Document("$sort", new Document("tweets", -1))
                    )).first();

            //System.out.println("Hashtags más frecuentes: " + docs.first());
            JOptionPane.showMessageDialog(null, "Hashtags más frecuentes: " + docs.get("_id"));

        } catch (MongoException me) {
            //System.out.println("Excepción: " + me.getMessage());
            JOptionPane.showMessageDialog(null, me.getMessage());
        }

    }

}
