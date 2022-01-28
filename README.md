## AdAnalysis
#Advertisement Analysis

## Business Model

Advertisement banners are displayed to users in a mobile application (`app_id`) in a country
(`country code`) from an advertiser (`advertiser_id`). When this happens, an `impression` event is
recorded and stored. Optionally, if the user clicks on the banner, a `click` event is recorded.
Revenue is generated **only** in the case of a click being triggered.

## Input

### Arguments

Application accept **2 lists of file names** with click and impression events.

### Impression event schema

- `id` (string): a UUID that identifies the impression.
- `app_id` (integer): an identifier of the application showing the impression.
- `country_code` (string): a 2-letter code for the country. It doesn't necessarily comply to any standard like ISO 3166.
- `advertiser_id` (integer): an identifier of the advertiser that bought the impression.

Example data can be found on `resources/impressions.json`.

```json
[
      {
        "app_id": 4,
        "advertiser_id": 15,
        "country_code": "IT",
        "id": "2ae9fd3f-4c70-4d9f-9fe0-98cb2f0b7521"
    },
  "..."
]
```


### Click event schema

- `impression_id` (string): a reference to the UUID of the impression where the click was produced.
- `revenue` (double): the quantity of money paid by the advertiser when the click is tracked.

Example data can be found on `resources/clicks.json`.

```json
[
      {
        "impression_id": "43bd7feb-3fea-40b4-a140-d01a35ec1f73",
        "revenue": "2.091225600111518"
    },
  "..."
]
```


## How to use

sample code to use the code is available on `src/main/scala/main.scala`, and you can find sample files and report results on '/resource/' directory.

### 1. Read events stored in JSON files

Below codes will be parse and load data to list objects,
Data processing and exception handling will be done in 2 level File and Node
Data validation rules are available on **schema** package

```scala
/* parse impressions.json data and return them as list of Impression object*/
  /* Duplicate ID with different appID and CountryCode can impact result when ID is not null so app will be retun error */
  var pImpressions=new JsonParser[Impression](sWorkingDir+"impressions.json");
  var lImpressions=pImpressions.Parse();

  /* parse clicks.json data and return them as list of Clicks object*/
  var pClicks=new JsonParser[Click](sWorkingDir+"clicks.json");
  var lClicks=pClicks.Parse();
```


### 2. Report1 Calculate metrics for some dimensions

The business team wants to check how some metrics perform depending on a few dimensions. For
example, they would like to check how applications are performing depending on the country. This
will be very useful for them, as they will be able to spot new opportunities or countries that are
performing poorly.

Metrics:

- Count of impressions
- Count of clicks
- Sum of revenue

Dimensions:

- `app_id`
- `country_code`

The object report.Report1Summery will generate this report with list of object that parser returned, the process will be export result on disk


```scala

/* use list of Impression and Clicks to generate summery report (Report1)*/
  var report1Summery=new Report1Summery(lImpressions,lClicks)
  report1Summery.Execute();
  report1Summery.ExportResult(sWorkingDir+"Report1.json")
```


Below is the format of report 1:

```json
[
  {
    "app_id": 1,
    "country_code": "US",
    "impressions": 102,
    "clicks": 12,
    "revenue": 10.2
  },
  "..."
]
```

### 3. Report 2 Make a recommendation for the top 5 advertiser_ids to display for each app and country combination.

The business team wants to know which are the top advertisers for each application and country.
This will allow them to focus their effort on this advertisers. To measure performance, we will
check for the highest rate of `revenue/impressions`. That is, the advertisers that, on average, pay
more per impression.

Output fields:

- `app_id`
- `country_code`
- `recomended_advertiser_ids` (list of top 5 advertiser ids with the highest revenue per impression rate in this application and country).

The object report. Report2Recommendation will generate this report with list of object that parser returned, the process will be export result on disk.
This process use spark to process data


```scala

  /* use list of Impression and Clicks to generate recommendation report (Report1) this process use spark*/
  var report2Recommendation=new Report2Recommendation(lImpressions,lClicks);
  report2Recommendation.Execute();
  report2Recommendation.ExportResult(sWorkingDir+"Report2.json");```


Below is the format of report 2:


```json
[
  {
    "app_id": 1,
    "country_code": "US",
    "recommended_advertiser_ids": [32, 12, 45, 4, 1]
  }
]
```


