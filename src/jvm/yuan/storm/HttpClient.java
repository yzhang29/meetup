package yuan.storm;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by yzhang29 on 4/30/15.
 */
public class HttpClient {
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
        Future<Response> f = asyncHttpClient.prepareGet("http://stream.meetup.com/2/open_events").execute(new AsyncCompletionHandler<Response>() {
            @Override
            public Response onCompleted(Response response) throws Exception {
                return response;
            }

            @Override
            public STATE onBodyPartReceived(HttpResponseBodyPart content) throws Exception
            {
                System.out.println("Feed: " + new String(content.getBodyPartBytes()));

                return STATE.CONTINUE;
            }
        });

    }
}
