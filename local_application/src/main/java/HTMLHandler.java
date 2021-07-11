import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HTMLHandler {
    private String basicTags = "<html>\n\t<head>\n\t<title>Sarcasm Analysis</title>\n\t</head>\n\t<body>$nextTag</body>\n</html>";
    private Map<String, String> colorMap = new HashMap();
    private final File outputHTML;

    // constructor - creating new html file
    public HTMLHandler(String outputFileName){

        this.outputHTML = new File(outputFileName + ".html");
        // map sentiment to specified color
        colorMap.put("0", "darkred");
        colorMap.put("1", "red");
        colorMap.put("2", "black");
        colorMap.put("3", "lightgreen");
        colorMap.put("4", "darkgreen");
    }

    public String createTitleHeader(String title) {
        return "<h4>" + title + "</h4>\n";
    }

    /**
     * Detect sarcasm - we decide that there is sarcasm if there's a difference of 2 between the sentiment and rating score
     * @param sentimentResult - the sentiment score
     * @param stars - the rating of the review
     * @return String "yes"/"no"
     */
    public String checkSarcasmDetection(String sentimentResult, Long stars){
        int sentimentResultInt = Integer.parseInt(sentimentResult);
        if(Math.abs(stars.intValue() - (sentimentResultInt + 1)) > 2) {
            return "Yes";
        }
        else {
            return "No";
        }
    }

    /**
     * Construct a line for each review, applying colors w.r.t sentiment score
     * @param reviewId
     * @param text
     * @param sentiment
     * @param entities
     * @param link
     * @param rating
     * @return the constructed review String
     */
    public String createReviewPar(String reviewId, String text, String sentiment, String entities, String link, Long rating) {
        return "<p>\n" +
                "   <div class=\"row\"> Review ID: " + reviewId + "</div>\n" +
                "   <div class=\"row\"> Review Text: "  + text + "</div>\n" +
                "   <div class=\"row\"> Review Rating: "  + rating.toString() + "</div>\n" +
                "   <div class=\"row\"> Review Sentiment Result: " + sentiment + "</div>\n" +
                "   <div class=\"row\"> Entities: " + entities + "</div>\n" +
                "   <div class=\"row\"> Link: <a href=\"url\" style=\"color: " + colorMap.get(sentiment) + ";\">" + link + "</a></div>\n" +
                "   <div class=\"row\"> Sarcasm Detection: " + checkSarcasmDetection(sentiment, rating) + "</div>\n" +
                "</p>\n";
    }


    public void removeLastNextTag(){
        basicTags = basicTags.replace("$nextTag", "");
    }

    /**
     *
     * @param title of the review
     * @param reviews related to the title
     * @throws IOException
     */
    public void addTitleAndReviews(String title, JSONArray reviews) throws IOException {
        StringBuilder htmlContent = new StringBuilder();    // content is a review with it's details
        String titleHeader = createTitleHeader(title);
        StringBuilder reviewsString = new StringBuilder();
        // construct a single line
        for(Object o : reviews) {
            JSONObject reviewJson = (JSONObject) o;
            String reviewId = (String) reviewJson.get("review_id");
            String text = (String) reviewJson.get("text");
            String sentiment = (String) reviewJson.get("sentiment");
            String entities = (String) reviewJson.get("entities");
            String link = (String) reviewJson.get("link");
            Long rating = (Long) reviewJson.get("rating");
            reviewsString.append(createReviewPar(reviewId, text, sentiment, entities, link, rating));
            reviewsString.append("<div class=\"row\">--------------------------------------------------------------------------------------------------</div>\n");
        }
        // add the line to the html content
        htmlContent.append(titleHeader).append(reviewsString.toString());
        basicTags = basicTags.replace("$nextTag", htmlContent.toString() + "$nextTag");
    }

    /**
     * write content to an html file
     * @throws IOException
     */
    public void writeToHtml() throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(this.outputHTML));
        bw.write(basicTags);
        bw.close();
    }

}
