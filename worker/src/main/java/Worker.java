import java.util.List;

public class Worker {

    public String name;
    public namedEntityRecognitionHandler namedEntityRecognitionHandler;
    public sentimentAnalysisHandler sentimentAnalysisHandler;

    public Worker (String name){
        this.namedEntityRecognitionHandler = new namedEntityRecognitionHandler();
        this.sentimentAnalysisHandler = new sentimentAnalysisHandler();
        this.name = name;
    }

    public int findSentiment(String review) {
        return sentimentAnalysisHandler.findSentiment(review);
    }

    public List<String> returnEntities(String review) {
        return namedEntityRecognitionHandler.returnEntities(review);
    }
}