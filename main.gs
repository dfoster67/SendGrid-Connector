// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @fileoverview Community Connector for SendGrid stats API to download email data. 
 * This can retrieve all of the basic stats available in the /stats endpoint.
 * See: https://sendgrid.com/docs/api-reference/ for more information.
 * Not developed or supported by SendGrid 
 */

var cc = DataStudioApp.createCommunityConnector();

// [START get_config]
// https://developers.google.com/datastudio/connector/reference#getconfig
function getConfig() {
  var config = cc.getConfig();

  config
    .newInfo()
    .setId('instructions')
    .setText('Enter the API Key from SendGrid.  For more information see https://sendgrid.com/docs/api-reference/.');

  config
    .newTextInput()
    .setId('apikey')
    .setName('Enter API KEY')
    .setHelpText('e.g. ABCDE12345')
    .setPlaceholder('ABCDE12345')
    .setAllowOverride(false);

  config.setDateRangeRequired(true);

  return config.build();
}
// [END get_config]

// [START get_schema]
function getFields() {
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

  fields
    .newDimension()
    .setId('date')
    .setName('Date')
    .setType(types.YEAR_MONTH_DAY);

  fields
    .newMetric()
    .setId('blocks')
    .setName('Blocks')
    .setDescription('The number of emails that were not allowed to be delivered by ISPs.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);

  fields
    .newMetric()
    .setId('bounce_drops')
    .setName('Bounce Drops')
    .setDescription('The number of emails that were dropped because of a bounce.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields
    .newMetric()
    .setId('bounces')
    .setName('Bounces')
    .setDescription('The number of emails that bounced instead of being delivered.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields
    .newMetric()
    .setId('clicks')
    .setName('Clicks')
    .setDescription('The number of links that were clicked in your emails.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields
    .newMetric()
    .setId('deferred')
    .setName('Deferred')
    .setDescription('The number of emails that temporarily could not be delivered.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields
    .newMetric()
    .setId('delivered')
    .setName('Delivered')
    .setDescription('The number of emails SendGrid was able to confirm were actually delivered to a recipient.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields
    .newMetric()
    .setId('invalid_emails')
    .setName('Invalid Emails')
    .setDescription('The number of recipients who had malformed email addresses or whose mail provider reported the address as invalid.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields
    .newMetric()
    .setId('opens')
    .setName('Opens')
    .setDescription('The total number of times your emails were opened by recipients.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields
    .newMetric()
    .setId('processed')
    .setName('Processed')
    .setDescription('Requests from your website, application, or mail client via SMTP Relay or the API that SendGrid processed.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields
    .newMetric()
    .setId('requests')
    .setName('Requests')
    .setDescription('The number of emails that were requested to be delivered.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields
    .newMetric()
    .setId('spam_report_drops')
    .setName('Spam Report Drops')
    .setDescription('The number of emails that were dropped due to a recipient previously marking your emails as spam.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields
    .newMetric()
    .setId('spam_reports')
    .setName('Spam Reports')
    .setDescription('The number of recipients who marked your email as spam.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields
    .newMetric()
    .setId('unique_clicks')
    .setName('Unique Clicks')
    .setDescription('The number of unique recipients who clicked links in your emails.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields
    .newMetric()
    .setId('unique_opens')
    .setName('Unique Opens')
    .setDescription('The number of unique recipients who opened your emails.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);

  fields
    .newMetric()
    .setId('unsubscribe_drops')
    .setName('Unsubscribe Drops')
    .setDescription('The number of emails dropped due to a recipient unsubscribing from your emails.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);

  fields
    .newMetric()
    .setId('unsubscribes')
    .setName('Unsubscribes')
    .setDescription('The number of recipients who unsubscribed from your emails.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);

  return fields;
}

// https://developers.google.com/datastudio/connector/reference#getschema
function getSchema(request) {
  return {schema: getFields().build()};
}
// [END get_schema]

// [START get_data]
// https://developers.google.com/datastudio/connector/reference#getdata
function getData(request) {
  request.configParams = validateConfig(request.configParams);

  var requestedFields = getFields().forIds(
    request.fields.map(function(field) {
      return field.name;
    })
  );

  try {
    var apiResponse = fetchDataFromApi(request);
    var data = getFormattedData(apiResponse, requestedFields);
  } catch (e) {
    cc.newUserError()
      .setDebugText('Error fetching data from API. Exception details: ' + e)
      .setText(
        'The connector has encountered an unrecoverable error. Please try again later, or file an issue if this error persists.'
      )
      .throwException();
  }

  return {
    schema: requestedFields.build(),
    rows: data
  };
}

/**
 * Gets response for UrlFetchApp.
 *
 * @param {Object} request Data request parameters.
 * @returns {string} Response text for UrlFetchApp.
 */
function fetchDataFromApi(request) {
  var url = [
    'https://api.sendgrid.com/v3/stats?aggregated_by=day&start_date=',
    request.dateRange.startDate,
    '&end_date=',
    request.dateRange.endDate
  ].join('');
  var header = { "Authorization": "Bearer " + request.configParams.apikey };
  var options = { "headers": header };
  var response = UrlFetchApp.fetch(url, options);
  return response;
}


/**
 * Formats the parsed response from external data source into correct tabular
 * format and returns only the requestedFields
 *
 * @param {Object} parsedResponse The response string from external data source
 *     parsed into an object in a standard format.
 * @param {Array} requestedFields The fields requested in the getData request.
 * @returns {Array} Array containing rows of data in key-value pairs for each
 *     field.
 */
function getFormattedData(response, requestedFields) {
  var data = [];
  response = JSON.parse(response);
  response.map(function(sendStats) {
    var date = sendStats.date;
    var dailyMetrics = sendStats.stats[0].metrics;
    var formattedData = formatData(requestedFields, date, dailyMetrics);
    data = data.concat(formattedData);
  });
  return data;
}
// [END get_data]

/**
 * isAdminUser - set to true to get additional debug information in error codes
 * https://developers.google.com/datastudio/connector/reference#isadminuser
 */
function isAdminUser() {
  return true;
}

/**
 * Validates config parameters and provides missing values.
 *
 * @param {Object} configParams Config parameters from `request`.
 * @returns {Object} Updated Config parameters.
 */
function validateConfig(configParams) {
  configParams = configParams || {};
// They set a default for .package, but we can't do that for apikey, so skip
//  configParams.package = configParams.package || DEFAULT_PACKAGE;

  return configParams;
}

/**
 * Formats a single row of data into the required format.
 *
 * @param {Object} requestedFields Fields requested in the getData request.
 * @param {string} date The date field for one row of data in the response.
 * @param {Object} dailyMetrics Contains the stats data for a specific date.
 * @returns {Object} Contains values for requested fields in predefined format.
 */
function formatData(requestedFields, date, dailyMetrics) {
  var row = requestedFields.asArray().map(function(requestedField) {
    switch (requestedField.getId()) {
      case 'date':
        return date.replace(/-/g, '');
      default:
        return dailyMetrics[requestedField.getId()];
    }
  });
  return {values: row};
}
