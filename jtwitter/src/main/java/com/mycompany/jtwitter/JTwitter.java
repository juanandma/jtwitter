/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.jtwitter;

/**
 *
 * @author JUANM
 */
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import twitter4j.Status;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import org.bson.Document;

public class JTwitter {

    static String consumerKey = "7ZmrVvtu4AeLRN9z7nyK2L8ZS";
    static String consumerSecret = "GM5J3fBGwGdNnCxGvhfjzCEsefddZTcDRc6nvOnNLZfDHM8Jv4";
    static String accessTokenCad = "1179786645253578753-rjQMKE5Dj7oZjZrttD2r4PJx63j6st";
    static String accessTokenSecret = "zF7SCEDswJvE7YWaxmmaiNKAccVvjr8jgew7tTCUtoYGt";

    MongoClient mongoClient;
    MongoDatabase database;
    MongoCollection<Document> col;

    private int tuits = 0;
    private final Object lock = new Object();

// Este método lee tuits en streaming
    public void leeTuitsStreaming(String busqueda, int iteraciones, boolean completo) {

        try {

            // Se crea un objeto de configuración al que se le asignan los valores de la credencial
            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
                    .setOAuthConsumerKey(consumerKey)
                    .setOAuthConsumerSecret(consumerSecret)
                    .setOAuthAccessToken(accessTokenCad)
                    .setOAuthAccessTokenSecret(accessTokenSecret)
                    .setJSONStoreEnabled(true)
                    .setTweetModeExtended(true);

            // Se inicia un streamer con la configuración
            TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

            // Se crea un listener para escuchar los tweets que van llegando
            StatusListener listener = new StatusListener() {
                @Override
                // El objeto status es el tweet completo
                // La función onStatus se lanza cada vez que se lee un nuevo tweet
                public void onStatus(Status status) {

                    tuits++;

                    if (completo) {
                        InsertaTweetCompleto(status,busqueda);
                    } else {

                        if (!status.isRetweet()) {

                            InsertaDatos("@" + status.getUser().getScreenName(), status.getCreatedAt(),
                                    status.getText(), "Tweet Original");
                        } else {
                            InsertaDatos("@" + status.getUser().getScreenName(), status.getCreatedAt(),
                                    "Es un RT", status.getRetweetedStatus().getText());
                        }
                    }

                    if (tuits >= iteraciones) {
                        synchronized (lock) {
                            lock.notify();
                        }
                        System.out.println("unlocked");
                    }

                }

                @Override
                public void onDeletionNotice(StatusDeletionNotice sdn) {
                    System.out.println("Status deletion notice");
                }

                @Override
                public void onTrackLimitationNotice(int i) {
                    System.out.println("Got track limitation notice:"
                            + i);
                }

                @Override
                public void onScrubGeo(long l, long l1) {
                    System.out.println("Scrub geo with:" + l + ":" + l1);
                }

                @Override
                public void onStallWarning(StallWarning sw) {
                    System.out.println("Stall warning");
                }

                @Override
                public void onException(Exception excptn) {
                    System.out.println("Exception occured:" + excptn.getMessage());
                    excptn.printStackTrace();
                }
            };

            // Se asigna el listener al stream
            twitterStream.addListener(listener);

            // Se crea un objeto de tipo FilterQuery
            FilterQuery filterQuery = new FilterQuery();
            filterQuery.track(busqueda);
            twitterStream.filter(filterQuery);

            try {
                synchronized (lock) {
                    lock.wait();
                }
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            System.out.println("----- " + tuits + " tuits extraidos y guardados. -----");
            twitterStream.shutdown();
            twitterStream.cleanUp();

        } catch (Exception te) {
            System.out.println("Excepción: " + te.getMessage());
        }

    }

// función para insertar cierta información de los tuits en una colección de MongoDB
// guardamos el nombre del usuario, la fecha, el texto del tuit y el texto si se trata
// de un retuit
    public void InsertaDatos(String userName, Date fecha, String tweet, String retweet) {
        // TODO
        // Crear un nuevo objeto de tipo Document con los valores de los
        // parámetros y añadirlo a la colección "col"

        Document doc = new Document();
        doc.append("userName", userName);
        doc.append("fecha", fecha);
        doc.append("tweet", tweet);
        doc.append("retweet", retweet);
        col.insertOne(doc);
    }

// función para insertar los tuits completos en formato JSON en una colección de MongoDB
    public void InsertaTweetCompleto(Status tweet, String palabraclave) {
        // TODO
        // Crear un nuevo objeto de tipo Gson
        // Convertir el tweet a jsonPretty
        // Convertirlo a tipo Document y añadirlo a la colección "col"
        Gson tuit = new Gson();
        //String json = TwitterObjectFactory.getRawJSON(tweet);
        
        String json = tuit.toJson(tweet);
        
        //Document dbObject = (Document) JSON.parse(json);
        Document dbObject = Document.parse(json);
        dbObject.append("palabraclave", palabraclave);
        col.insertOne(dbObject);
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

    public static void main(String[] args) throws UnknownHostException {
        JTwitter jt = new JTwitter();
        if (args.length > 0) {
            jt.initMongoDB();
            String palabrasBusqueda = args[0];

            int tuits = 1;
            //modo 0: extrae 5 campos del tuit
            //modo 1: extrae el tuit completo
            boolean completo = false;
            if (args.length > 1) {
                tuits = Integer.parseInt(args[1]);

            }
            if (args.length > 2) {
                if (args[2].equalsIgnoreCase("completo")) {
                    completo = true;
                }

            }

            if (tuits > 0) {

                jt.leeTuitsStreaming(palabrasBusqueda, tuits, completo);

            }
            jt.shutdownMongoDB();

        }

    }

}
