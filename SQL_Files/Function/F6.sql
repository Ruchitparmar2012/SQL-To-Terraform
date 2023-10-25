
CREATE OR REPLACE FUNCTION DISC_DEV.JAZZHR.FETCH_EMPLOYEE_DATA("PAGE_SIZE" FLOAT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS '
  var api_url = "https://api.resumatorapi.com/v1/applicants/from_apply_date/2023-05-29/to_apply_date/2023-06-05/page/";
  // Replace with your actual API URL
  
  var headers = {
    "Content-Type": "application/json"
  };

  var data = [];
  var pageNumber = 1;
  var pageSize = Math.max(1, page_size);
  var responseData;

  do {
    var apiUrlWithPage = api_url + pageNumber + "/?apikey=anA7oHrk2d46yeq3RQ96ZvlrkOBpHYjT";
    var response = snowflake.execute({
      sqlText: "SELECT SYSTEM$HTTPGET(:1, :2)",
      binds: [apiUrlWithPage, JSON.stringify(headers)],
      resultFormat: "JSON"
    });
  
    if (response.next()) {
      responseData = JSON.parse(response.getColumnValue(1));
      data = data.concat(responseData);
    }
  
    pageNumber++;

    // Add a delay of 1 second between API calls
    snowflake.execute({ sqlText: "SYSTEM$WAIT(1)" });

  } while (responseData.length > 0);
  
  return data;
  ';