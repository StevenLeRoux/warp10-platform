package io.metrics.directory;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticClient {
    
    private static ElasticClient getElasticClient = new ElasticClient();
    
    private RestHighLevelClient client;
    
    /**
     * Open elastic client as Singleton
     * Elastic client manage it's own thread
     */
    private ElasticClient() { 
    }
    
    /** 
     * Set client as Singleton based on init properties
     * @param hostsNames
     * @param user
     * @param pwd
     */
    public void init(String hostsNames, String user, String pwd) {
        // Dynamically load Elastic hosts from user parameter split per ","
        String[] hosts = hostsNames.split(",");

        // Fill HttpHost array with all specified elastic hosts
        HttpHost[] httpsHost = new HttpHost[hosts.length];
        for (int i = 0; i< hosts.length; i++) {
            HttpHost h = HttpHost.create(hosts[i].replaceAll("\\s+",""));
            httpsHost[i] = h;
        }

        // Manage Elastic credential
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, pwd));

        // Initialize High level Elastic Rest builder client
        RestClientBuilder restClientBuilder = RestClient.builder(httpsHost);
        
        restClientBuilder.setMaxRetryTimeoutMillis(1000000);
        
        // set auth
        restClientBuilder.setHttpClientConfigCallback(getHttpConfig(credentialsProvider));
        
        // Open current Elastic client
        this.client = new RestHighLevelClient(
                restClientBuilder);
    }
    
    /** 
     * Config callback method
     * @param credentialsProvider
     * @return
     */
    private RestClientBuilder.HttpClientConfigCallback getHttpConfig(CredentialsProvider credentialsProvider) {
        RestClientBuilder.HttpClientConfigCallback httpConfig = new RestClientBuilder.HttpClientConfigCallback() {
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        };
        return httpConfig;
    }
    
    /**
     * Get singleton instance
     * @return
     */
    public static ElasticClient getInstance( ) {
       return getElasticClient;
    }
    
    /**
     * Get current Elastic client
     * @return
     */
    public RestHighLevelClient getClient() {
        return this.client;
     }

}
