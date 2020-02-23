package com.vamsee.kafka.kafka_producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	
	
	public Client run(BlockingQueue<String> msgQueue) {
		
	
		System.out.println("Twitter streaming api setup");

		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		List<String> terms = new ArrayList<String>();
		terms.add("DemDebate");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(VamseeConstants.consumerKey, VamseeConstants.consumerSecret, VamseeConstants.token, VamseeConstants.secret);

		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
				
				return hosebirdClient;
				
				
	}

	 public static void main( String[] args ) {
		 
		// TODO Auto-generated method stub
		System.out.println("Started");
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

		
		Client hosebirdClient =	new TwitterProducer().run(msgQueue);
		// Attempts to establish a connection.
		hosebirdClient.connect();
		
		
		System.out.println("Setup Connected");

		
		while (!hosebirdClient.isDone()) {
			String msg = null;
			  try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println("error"+ e);
			}
			 if(msg != null) {
				 System.out.println(msg);
			 }
			}
		System.out.println("End of app");
		
	}


}
