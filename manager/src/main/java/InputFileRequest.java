import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

enum Status {
    PROCESSING,
    DONE
}

/**
 * Represents a single review
 */
class ReviewRequest {

    public String review_id;
    public String text;
    public Status status;
    public String review_link;
    public Long rating;
    public JSONObject summeryJson;

    @SuppressWarnings("unchecked")
    public ReviewRequest(String review_id, String text, String review_link, Long rating) {
        this.review_id = review_id;
        this.text = text;
        this.summeryJson = new JSONObject();
        this.review_link = review_link;
        this.rating = rating;
        summeryJson.put("link", review_link);
        summeryJson.put("rating", rating);
        status = Status.PROCESSING;

    }
}

/**
 * Represents a review title
 */
class TitleReviewRequest {

    public JSONObject summeryJson;
    public String title;
    public ConcurrentHashMap<String, ReviewRequest> reviewRequests;

    @SuppressWarnings("unchecked")
    public TitleReviewRequest(String title, JSONArray  reviews) {
        this.title = title;
        this.summeryJson = new JSONObject();
        this.summeryJson.put("title", title);
        this.summeryJson.put("reviews", new JSONArray());
        this.reviewRequests = new ConcurrentHashMap<>();
        for (Object o : reviews) {
            JSONObject reviewJson = (JSONObject) o;
            String review_id = (String) reviewJson.get("id");
            String review_text = (String) reviewJson.get("text");
            String review_link = (String) reviewJson.get("link");
            Long rating = (Long) (reviewJson.get("rating"));
            reviewRequests.put(review_id, new ReviewRequest(review_id, review_text, review_link, rating));
        }
    }

    public ReviewRequest getReviewRequest (String review_id) {
        return reviewRequests.get(review_id);
    }

    public void addReviews(JSONArray reviews) {
        for (Object o : reviews) {
            JSONObject reviewJson = (JSONObject) o;
            String review_id = (String) reviewJson.get("id");
            String review_text = (String) reviewJson.get("text");
            String review_link = (String) reviewJson.get("link");
            Long rating = (Long) (reviewJson.get("rating"));
            reviewRequests.put(review_id, new ReviewRequest(review_id, review_text, review_link, rating));
        }
    }

    @SuppressWarnings("unchecked")
    public void prepare() {
        JSONArray array = (JSONArray) summeryJson.get("reviews");
        for(ReviewRequest reviewRequest : reviewRequests.values()) {
            array.add(reviewRequest.summeryJson);
        }
    }
}

/**
 * Represents an input file
 */
public class InputFileRequest {
    public Status status;
    public String inputUrl;
    public String inputFileName;
    public String appID;
    public AtomicInteger msgCount;
    public ConcurrentHashMap<String, TitleReviewRequest> titleReviewRequests;
    public JSONArray summeryFile;

    public InputFileRequest(String appID, String inputFileName, String inputUrl, ConcurrentHashMap<String,TitleReviewRequest> titleReviewRequests) {
        this.appID = appID;
        this.inputFileName = inputFileName;
        this.inputUrl = inputUrl;
        this.titleReviewRequests = titleReviewRequests;
        this.status = Status.PROCESSING;
        this.summeryFile = new JSONArray();
        this.msgCount = new AtomicInteger(0);
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public TitleReviewRequest getTitleReviewRequest(String title){
        return titleReviewRequests.get(title);
    }
    @SuppressWarnings("unchecked")
    public void prepare() {
        for(TitleReviewRequest titleRequest : titleReviewRequests.values()) {
            titleRequest.prepare();
            summeryFile.add(titleRequest.summeryJson);
        }
    }

    public ConcurrentHashMap<String, TitleReviewRequest> getTitleReviewRequests() {
        return this.titleReviewRequests;
    }

}
