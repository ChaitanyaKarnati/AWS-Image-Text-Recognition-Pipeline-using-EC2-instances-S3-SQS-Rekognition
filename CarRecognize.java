import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.util.Map;

public class CarRecognize {

    private static final Logger logger = LoggerFactory.getLogger(CarRecognize.class);
    private static final String REGION = "us-east-1";
    private static final String BUCKET_NAME = "njit-cs-643";
    private static final String QUEUE_NAME = "car.fifo";
    private static final String QUEUE_GROUP = "group1";
    private static final int MAX_RETRIES = 5;

    public static void main(String[] args) {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(REGION).build();
        AmazonRekognition rek = AmazonRekognitionClientBuilder.standard().withRegion(REGION).build();
        AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(REGION).build();

        try {
            processBucketImages(s3, rek, sqs);
        } catch (Exception e) {
            logger.error("Error processing bucket images", e);
        }
    }

    private static void processBucketImages(AmazonS3 s3, AmazonRekognition rek, AmazonSQS sqs) {
        String queueUrl = getQueueUrl(sqs);

        ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(BUCKET_NAME).withMaxKeys(10);
        ListObjectsV2Result result = s3.listObjectsV2(request);

        for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
            if (!isSupportedImageFile(objectSummary.getKey())) {
                continue;
            }

            logger.info("Processing image: {}", objectSummary.getKey());
            if (isImageOfCar(s3, rek, objectSummary.getKey())) {
                sendMessageToQueue(sqs, queueUrl, objectSummary.getKey());
            }
        }

        sendMessageToQueue(sqs, queueUrl, "-1");
    }

    private static boolean isSupportedImageFile(String key) {
        // Logic to check if file is of supported image type e.g. .jpg, .png
        // This limits the Rekognition calls to only supported image types
    }

    private static boolean isImageOfCar(AmazonS3 s3, AmazonRekognition rek, String key) {
        DetectLabelsRequest detectLabelsRequest = new DetectLabelsRequest()
                .withImage(new Image().withS3Object(new S3Object().withBucket(BUCKET_NAME).withName(key)))
                .withMinConfidence(90f);
        
        int retries = 0;
        while (retries < MAX_RETRIES) {
            try {
                for (Label label : rek.detectLabels(detectLabelsRequest).getLabels()) {
                    if ("Car".equals(label.getName())) {
                        return true;
                    }
                }
                break;  // Exit if the call was successful
            } catch (Exception e) {
                retries++;
                if (retries >= MAX_RETRIES) {
                    logger.error("Max retries reached for Rekognition. Failing for image: {}", key);
                    throw e;
                }
                try {
                    long backoffTime = (long) (Math.pow(2, retries) * 1000); // Exponential backoff
                    logger.warn("Rate limited by Rekognition. Retrying after {} ms. Attempt {}/{} for image: {}",
                            backoffTime, retries, MAX_RETRIES, key);
                    TimeUnit.MILLISECONDS.sleep(backoffTime);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Thread was interrupted during backoff", ie);
                }
            }
        }
        return false;
    }

    private static void sendMessageToQueue(AmazonSQS sqs, String queueUrl, String message) {
        SendMessageRequest sendMsgRequest = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(message)
                .withMessageGroupId(QUEUE_GROUP);
        sqs.sendMessage(sendMsgRequest);
    }

  private static String getQueueUrl(AmazonSQS sqs) {
        String queueUrl = "";
        Map<String, String> attributes = Map.of(
                "FifoQueue", "true",
                "ContentBasedDeduplication", "true"
        );

        try {
            queueUrl = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
        } catch (Exception e) {
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_NAME).withAttributes(attributes);
            queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        }
        return queueUrl;
    }
}