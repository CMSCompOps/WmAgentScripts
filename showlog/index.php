<?php

ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

function search_logs($q, $size, $m) {
  $url = 'https://es-unified7.cern.ch:443/es/unified-logs/_doc/_search?size=' . $size;
  ###$goodquery = '{"query": {"bool": {"must": [{"wildcard": {"meta": "*' . $q . '*"}}]}}, "sort": [{"timestamp": "desc"}], "_source": ["text", "subject", "date", "meta"]}';
  if (!$m)
     $goodquery = '{"query": {"wildcard": {"meta": "*' . $q . '*"}}, "sort": [{"timestamp": "desc"}], "_source": ["text", "subject", "date", "meta"]}';
  else {
     $goodquery = '{"query": {"bool": {"must": [{"wildcard": {"meta": "*' . $q . '*"}}, {"term" : {"subject" : "' . $m .'" }}]}}, "sort": [{"timestamp": "desc"}], "_source": ["text", "subject", "date", "meta"]}';
  }
  $headers = "Accept: application/json\r\n" .
    "Content-Type: application/json\r\n" . 
    "Authorization: Basic dW5pZmllZF9ybzpkNVNkZmFzZGxoajUlQVNkZnNkanNhU0RBU0RmYWhhRGZhc1NkZmg1\r\n";

  $opts = array(
                'http' => array(
                                'header' => $headers,
                                'method' => 'POST',
                                'content' => $goodquery
                                )
                );

  $context = stream_context_create($opts);
  $result = json_decode(file_get_contents($url, false, $context), true);

  return $result['hits']['hits'];
}

function get($param, $default) {
  return isset($_GET[$param]) ? $_GET[$param] : $default;
}

$query = get('search', '');
$module = get('module', '');
$limit = (int)(get('limit', 50));
$size = (int)(get('size', 1000));
$keep_duplicates = (bool)(get('all', false));

$checked = $keep_duplicates ? ' checked' : '';

$formtext = sprintf('<form>Submit query: <input type="text" name="search"> ' .
                    'Module: <input type="text" name="module" value="%s"> ' .
                    'Logs Limit: <input type="text" name="limit" value="%d"> ' .
                    'Elastic Search Size: <input type="text" name="size" value="%d"> ' .
                    'Keep Duplicates: <input type="checkbox" name="all"%s> ' .
                    '<input type="submit"></form>',
                    $module, $limit, $size, $checked);

# Get form page
if ($query == '')
  echo $formtext;
else {
  $o = search_logs($query, $size, $module);
  if (count($o) == 0)
    echo 'No logs were found!<br>' . $formtext;

  else {
    $texts = array();
    $logs = array();

    foreach($o as $i) {
      if (count($texts) > $limit)
        break;
      if(in_array($i['_source']['text'], $texts) !== false and !$keep_duplicates)
        continue;

      if (!$module or $i['_source']['subject'] == $module) {
        array_push($logs,
                   array('subject' => $i['_source']['subject'],
                         'date' => $i['_source']['date'],
                         'text' => explode("\n", $i['_source']['text']))
                   );

        array_push($texts, $i['_source']['text']);
      }
    }

    include 'table.html';
  }
}

?>
