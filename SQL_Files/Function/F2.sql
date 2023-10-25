
CREATE OR REPLACE FUNCTION DISC_DEV.PUBLIC.DATERANGE("STARTDATE" VARCHAR(16777216), "ENDDATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS '
  var dates = [],
      currentDate = Date.parse(STARTDATE),
      addDays = function(days) {
        var date = new Date(this.valueOf());
        date.setDate(date.getDate() + days);
        return date;
      };
var formatDate = function(date) {
    var d = new Date(date),
        month = '''' + (d.getMonth() + 1),
        day = '''' + d.getDate(),
        year = d.getFullYear();
 
    if (month.length < 2) month = ''0'' + month;
    if (day.length < 2) day = ''0'' + day;
 
    return [year, month, day].join(''-'');
}
  while (currentDate <= Date.parse(ENDDATE)) {
    dates.push(formatDate(currentDate));
    currentDate = addDays.call(currentDate, 1);
  }
  return "(''"+dates.join("'',''")+"'')";
';


