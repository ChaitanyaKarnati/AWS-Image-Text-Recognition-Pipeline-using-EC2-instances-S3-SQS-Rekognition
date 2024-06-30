import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TextRecognize {
    
    private static final String REGION = "us-east-1";
    private static final String BUCKET_NAME = "njit-cs-643";
    private static final String QUEUE_NAME = "car.fifo";

    public static void main(String[] args) {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(REGION).build();
        AmazonRekognition rek = AmazonRekognitionClientBuilder.standard().withRegion(REGION).build();
        AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(REGION).build();

        TextRecognize(s3, rek, sqs);
    }

    private static void TextRecognize(AmazonS3 s3, AmazonRekognition rek, AmazonSQS sqs) {
        
        String queueUrl;
        
        // Ensure the queue exists
        while (true) {
            ListQueuesResult listQueuesResult = sqs.listQueues(new ListQueuesRequest().withQueueNamePrefix(QUEUE_NAME));
            if (listQueuesResult.getQueueUrls().size() > 0) {
                break;
            }
        }
        
        queueUrl = sqs.getQueueUrl(new GetQueueUrlRequest(QUEUE_NAME)).getQueueUrl();
        
        Map<String, String> outputs = new HashMap<>();
        
        while (true) {
            ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(new ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(1));
            
            if (receiveMessageResult.getMessages().isEmpty()) {
                continue;
            }
            
            Message message = receiveMessageResult.getMessages().get(0);
            String label = message.getBody();

            if ("-1".equals(label)) {
                break;
            }

            System.out.println("Processing car image with text from " + BUCKET_NAME + " S3 bucket: " + label);

            DetectTextRequest detectTextRequest = new DetectTextRequest().withImage(new Image().withS3Object(new com.amazonaws.services.rekognition.model.S3Object().withBucket(BUCKET_NAME).withName(label)));
            DetectTextResult detectTextResult = rek.detectText(detectTextRequest);
            
            StringBuilder detectedText = new StringBuilder();
            for (TextDetection textDetection : detectTextResult.getTextDetections()) {
                if ("WORD".equals(textDetection.getType())) {
                    detectedText.append(textDetection.getDetectedText()).append(" ");
                }
            }
            
            outputs.put(label, detectedText.toString());

            // Delete the processed message
            sqs.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
        }

        // Write results to a file
        try (BufferedWriter outfile = new BufferedWriter(new FileWriter("output.txt"))) {
            for (Map.Entry<String, String> entry : outputs.entrySet()) {
                outfile.write(entry.getKey() + ": " + entry.getValue() + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Results written to file output.txt");
    }
}